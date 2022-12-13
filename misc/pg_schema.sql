-- Currently there is no schema versioning/management
-- This entire script is intended to run against an empty DB as an initialization step
-- It is *SAFE* to run it against a live database with existing data
--
--   psql service=XYZ < misc/pg_schema.sql
--

CREATE SCHEMA IF NOT EXISTS egd;

CREATE OR REPLACE
  FUNCTION egd.big_now() RETURNS BIGINT
LANGUAGE sql PARALLEL SAFE VOLATILE STRICT AS $$
  SELECT ( EXTRACT( EPOCH from CLOCK_TIMESTAMP() )::NUMERIC * 1000000000 )::BIGINT
$$;

CREATE OR REPLACE
  FUNCTION egd.coarse_epoch(INTEGER) RETURNS INTEGER
LANGUAGE sql PARALLEL SAFE IMMUTABLE STRICT AS $$
  -- round down to the nearest week
  SELECT ( $1 / ( 7 * 2880 ) )::INTEGER * 7 * 2880
$$;

CREATE OR REPLACE
  FUNCTION egd.ts_from_epoch(INTEGER) RETURNS TIMESTAMP WITH TIME ZONE
LANGUAGE sql PARALLEL SAFE IMMUTABLE STRICT AS $$
  SELECT TIMEZONE( 'UTC', TO_TIMESTAMP( $1 * 30::BIGINT + 1598306400 ) )
$$;

CREATE OR REPLACE
  FUNCTION egd.epoch_from_ts(TIMESTAMP WITH TIME ZONE) RETURNS INTEGER
LANGUAGE sql PARALLEL SAFE IMMUTABLE STRICT AS $$
  SELECT ( EXTRACT( EPOCH FROM $1 )::BIGINT - 1598306400 ) / 30
$$;

CREATE OR REPLACE
  FUNCTION egd.replica_expiration_cutoff_epoch() RETURNS INTEGER
LANGUAGE sql PARALLEL RESTRICTED STABLE AS $$
  SELECT egd.epoch_from_ts( DATE_TRUNC( 'day', NOW() + '45 days'::INTERVAL ) )
$$;

CREATE OR REPLACE
  FUNCTION egd.proposal_deduplication_recent_cutoff_epoch() RETURNS INTEGER
LANGUAGE sql PARALLEL RESTRICTED STABLE AS $$
  SELECT egd.epoch_from_ts( DATE_TRUNC( 'hour', NOW() - '1 days'::INTERVAL ) )
$$;

CREATE OR REPLACE
  FUNCTION egd.valid_cid_v1(TEXT) RETURNS BOOLEAN
    LANGUAGE sql PARALLEL SAFE IMMUTABLE STRICT
AS $$
  SELECT SUBSTRING( $1 FROM 1 FOR 2 ) = 'ba'
$$;

CREATE OR REPLACE
  FUNCTION egd.valid_cid(TEXT) RETURNS BOOLEAN
    LANGUAGE sql PARALLEL SAFE IMMUTABLE STRICT
AS $$
  SELECT ( SUBSTRING( $1 FROM 1 FOR 2 ) = 'ba' OR SUBSTRING( $1 FROM 1 FOR 2 ) = 'Qm' )
$$;


CREATE OR REPLACE
  FUNCTION egd.update_entry_timestamp() RETURNS TRIGGER
    LANGUAGE plpgsql
AS $$
BEGIN
  NEW.entry_last_updated = NOW();
  RETURN NEW;
END;
$$;


CREATE TABLE IF NOT EXISTS egd.metrics (
  name TEXT NOT NULL CONSTRAINT metric_name_lc CHECK ( name ~ '^[a-z0-9_]+$' ),
  dimensions TEXT[][] NOT NULL,
  description TEXT NOT NULL,
  value BIGINT,
  collected_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CLOCK_TIMESTAMP(),
  collection_took_seconds NUMERIC NOT NULL,
  CONSTRAINT metric_uidx UNIQUE ( name, dimensions )
);
CREATE TABLE IF NOT EXISTS egd.metrics_log (
  name TEXT NOT NULL,
  dimensions TEXT[][] NOT NULL,
  value BIGINT,
  collected_at TIMESTAMP WITH TIME ZONE NOT NULL,
  collection_took_seconds NUMERIC NOT NULL,
  CONSTRAINT metrics_log_metric_fk FOREIGN KEY ( name, dimensions ) REFERENCES egd.metrics ( name, dimensions )
);
CREATE INDEX IF NOT EXISTS metrics_log_collected_name_dim ON egd.metrics_log ( collected_at, name );
CREATE OR REPLACE
  FUNCTION egd.record_metric_change() RETURNS TRIGGER
    LANGUAGE plpgsql
AS $$
BEGIN
  INSERT INTO egd.metrics_log ( name, dimensions, value, collected_at, collection_took_seconds ) VALUES ( NEW.name, NEW.dimensions, NEW.value, NEW.collected_at, NEW.collection_took_seconds );
  RETURN NULL;
END;
$$;
CREATE OR REPLACE TRIGGER trigger_store_metric_logs
  AFTER INSERT OR UPDATE ON egd.metrics
  FOR EACH ROW
  EXECUTE PROCEDURE egd.record_metric_change()
;

CREATE TABLE IF NOT EXISTS egd.global(
  singleton_row BOOL NOT NULL UNIQUE CONSTRAINT single_row_in_table CHECK ( singleton_row IS TRUE ),
  metadata JSONB NOT NULL
);
INSERT INTO egd.global ( singleton_row, metadata ) VALUES ( true, '{ "schema_version":{ "major": 1, "minor": 0 } }' ) ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS egd.tenants (
  tenant_id SMALLSERIAL NOT NULL UNIQUE,
  tenant_name TEXT NOT NULL UNIQUE,
  tenant_meta JSONB NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS egd.datasets (
  dataset_id SMALLSERIAL NOT NULL UNIQUE,
  dataset_slug TEXT NOT NULL UNIQUE CONSTRAINT dataset_slug_lc CHECK ( dataset_slug ~ '^[a-z0-9\-]+$' ),
  dataset_meta JSONB NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS egd.tenants_datasets (
  tenant_id SMALLINT NOT NULL REFERENCES egd.tenants ( tenant_id ) ON UPDATE CASCADE,
  dataset_id SMALLINT NOT NULL REFERENCES egd.datasets ( dataset_id ) ON UPDATE CASCADE,
  tenant_dataset_meta JSONB NOT NULL DEFAULT '{}',
  CONSTRAINT tenants_datasets_singleton UNIQUE ( tenant_id, dataset_id )
);

CREATE TABLE IF NOT EXISTS egd.pieces (
  piece_id BIGINT NOT NULL UNIQUE,
  piece_cid TEXT NOT NULL UNIQUE CONSTRAINT piece_valid_pcid CHECK ( egd.valid_cid_v1( piece_cid ) ),
  piece_log2_size SMALLINT NOT NULL CONSTRAINT piece_valid_size CHECK ( piece_log2_size > 0 ),
  proposal_label TEXT CONSTRAINT piece_valid_lcid CHECK ( egd.valid_cid( proposal_label ) ),
  piece_meta JSONB NOT NULL DEFAULT '{}',
  CONSTRAINT pieces_piece_size_key UNIQUE ( piece_log2_size, piece_id ), -- proposals FK
  CONSTRAINT pieces_piece_id_cid_key UNIQUE ( piece_id, piece_cid ) -- published_deals FK
);
CREATE INDEX IF NOT EXISTS pieces_unproven_size ON egd.pieces ( piece_id ) WHERE ( NOT COALESCE( (piece_meta->'size_proven_correct')::BOOL, false) );
CREATE OR REPLACE
  FUNCTION egd.prefill_piece_id() RETURNS TRIGGER
    LANGUAGE plpgsql
AS $$
BEGIN
  NEW.piece_id = ( SELECT COALESCE( MIN( piece_id ), 0 ) - 1 FROM egd.pieces );
  RETURN NEW;
END;
$$;
CREATE OR REPLACE TRIGGER trigger_fill_next_piece_id
  BEFORE INSERT ON egd.pieces
  FOR EACH ROW
  WHEN ( NEW.piece_id IS NULL )
  EXECUTE PROCEDURE egd.prefill_piece_id()
;


CREATE TABLE IF NOT EXISTS egd.datasets_pieces (
  piece_id BIGINT NOT NULL REFERENCES egd.pieces ( piece_id ) ON UPDATE CASCADE,
  dataset_id SMALLINT NOT NULL REFERENCES egd.datasets ( dataset_id ) ON UPDATE CASCADE,
  dataset_piece_meta JSONB NOT NULL DEFAULT '{}',
  CONSTRAINT datasets_pieces_singleton UNIQUE ( piece_id, dataset_id )
);

CREATE TABLE IF NOT EXISTS egd.clients (
  client_id INTEGER UNIQUE NOT NULL,
  tenant_id SMALLINT REFERENCES egd.tenants ( tenant_id ) ON UPDATE CASCADE,
  client_address TEXT UNIQUE CONSTRAINT client_valid_address CHECK ( SUBSTRING( client_address FROM 1 FOR 2 ) IN ( 'f1', 'f3', 't1', 't3' ) ),
  client_meta JSONB NOT NULL DEFAULT '{}',
  CONSTRAINT tenant_has_robust CHECK (
    tenant_id IS NULL
      OR
    client_address IS NOT NULL
  )
);
CREATE INDEX IF NOT EXISTS clients_tenant_idx ON egd.clients ( tenant_id );

CREATE TABLE IF NOT EXISTS egd.providers (
  provider_id INTEGER UNIQUE NOT NULL,
  org_id SMALLINT NOT NULL DEFAULT 0,
  city_id SMALLINT NOT NULL DEFAULT 0,
  country_id SMALLINT NOT NULL DEFAULT 0,
  continent_id  SMALLINT NOT NULL DEFAULT 0,
  provider_meta JSONB NOT NULL DEFAULT '{}',
  CONSTRAINT consistent_location CHECK (
    ( org_id = 0 AND city_id = 0 AND country_id = 0 AND continent_id = 0 )
      OR
    ( org_id > 0 AND city_id > 0 AND country_id > 0 AND continent_id > 0 )
  )
);

CREATE TABLE IF NOT EXISTS egd.providers_info (
  provider_id INTEGER UNIQUE NOT NULL REFERENCES egd.providers ( provider_id ),
  provider_last_polled TIMESTAMP WITH TIME ZONE NOT NULL,
  info_last_updated TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
  info_dialing_took_msecs INTEGER,
  info_dialing_peerid TEXT,
  info JSONB NOT NULL DEFAULT '{}'
);
CREATE TABLE IF NOT EXISTS egd.providers_info_log (
  provider_id INTEGER NOT NULL REFERENCES egd.providers ( provider_id ),
  info_entry_created TIMESTAMP WITH TIME ZONE NOT NULL,
  info_dialing_took_msecs INTEGER,
  info_dialing_peerid TEXT,
  info JSONB NOT NULL DEFAULT '{}'
);
CREATE OR REPLACE
  FUNCTION egd.record_provider_info_change() RETURNS TRIGGER
    LANGUAGE plpgsql
AS $$
BEGIN
  INSERT INTO egd.providers_info_log (
    provider_id, info_entry_created, info_dialing_took_msecs, info_dialing_peerid, info
  ) VALUES(
    NEW.provider_id, NEW.provider_last_polled, NEW.info_dialing_took_msecs, NEW.info_dialing_peerid, NEW.info
  );
  UPDATE egd.providers_info SET
    info_last_updated = NEW.provider_last_polled
  WHERE provider_id = NEW.provider_id;
  RETURN NULL;
END;
$$;
CREATE OR REPLACE TRIGGER trigger_new_provider_info
  AFTER INSERT ON egd.providers_info
  FOR EACH ROW
  EXECUTE PROCEDURE egd.record_provider_info_change()
;
CREATE OR REPLACE TRIGGER trigger_update_provider_info
  AFTER UPDATE ON egd.providers_info
  FOR EACH ROW
  WHEN ( OLD.info != NEW.info )
  EXECUTE PROCEDURE egd.record_provider_info_change()
;


CREATE TABLE IF NOT EXISTS egd.tenants_providers (
  provider_id INTEGER NOT NULL REFERENCES egd.providers ( provider_id ),
  tenant_id SMALLINT NOT NULL REFERENCES egd.tenants ( tenant_id ) ON UPDATE CASCADE,
  tenant_provider_meta JSONB NOT NULL DEFAULT '{}',
  CONSTRAINT tenants_providers_singleton UNIQUE ( tenant_id, provider_id )
);


CREATE TABLE IF NOT EXISTS egd.requests (
  provider_id INTEGER NOT NULL REFERENCES egd.providers ( provider_id ),
  request_uuid UUID UNIQUE NOT NULL DEFAULT gen_random_uuid(),
  entry_created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
  request_dump JSONB NOT NULL,
  request_meta JSONB NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS requests_entry_created ON egd.requests ( entry_created);
CREATE OR REPLACE
  FUNCTION egd.init_authed_sp() RETURNS TRIGGER
    LANGUAGE plpgsql
AS $$
BEGIN
  INSERT INTO egd.providers( provider_id ) VALUES ( NEW.provider_id ) ON CONFLICT DO NOTHING;
  RETURN NEW;
END;
$$;
CREATE OR REPLACE TRIGGER trigger_create_related_sp
  BEFORE INSERT ON egd.requests
  FOR EACH ROW
  EXECUTE PROCEDURE egd.init_authed_sp()
;


CREATE TABLE IF NOT EXISTS egd.published_deals (
  deal_id BIGINT UNIQUE NOT NULL CONSTRAINT deal_valid_id CHECK ( deal_id > 0 ),
  piece_id BIGINT NOT NULL,
  piece_cid TEXT NOT NULL,
  claimed_log2_size BIGINT NOT NULL CONSTRAINT piece_valid_size CHECK ( claimed_log2_size > 0 ),
  provider_id INTEGER NOT NULL REFERENCES egd.providers ( provider_id ),
  client_id INTEGER NOT NULL REFERENCES egd.clients ( client_id ),
  label BYTEA NOT NULL,
  decoded_label TEXT CONSTRAINT deal_valid_label_cid CHECK ( egd.valid_cid( decoded_label ) ),
  is_filplus BOOL NOT NULL,
  status TEXT NOT NULL,
  published_deal_meta JSONB NOT NULL DEFAULT '{}',
  start_epoch INTEGER NOT NULL CONSTRAINT deal_valid_start CHECK ( start_epoch > 0 ),
  end_epoch INTEGER NOT NULL CONSTRAINT deal_valid_end CHECK ( end_epoch > 0 ),
  sector_start_epoch INTEGER CONSTRAINT deal_valid_sector_start CHECK ( sector_start_epoch > 0 ),
  entry_created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
  CONSTRAINT piece_id_cid_fkey FOREIGN KEY ( piece_id, piece_cid ) REFERENCES egd.pieces ( piece_id, piece_cid ) ON UPDATE CASCADE
);
CREATE INDEX IF NOT EXISTS published_deals_piece_id_idx ON egd.published_deals ( piece_id );
CREATE INDEX IF NOT EXISTS published_deals_status ON egd.published_deals ( status, piece_id, is_filplus, provider_id );
CREATE INDEX IF NOT EXISTS published_deals_active ON egd.published_deals ( piece_id ) INCLUDE ( claimed_log2_size ) WHERE ( status = 'active' );
CREATE INDEX IF NOT EXISTS published_deals_fildag ON egd.published_deals ( piece_id ) WHERE (
    status = 'active'
      AND
    decoded_label IS NOT NULL
      AND
    decoded_label NOT LIKE 'baga6ea4sea%'
);
CREATE INDEX IF NOT EXISTS published_deals_live ON egd.published_deals ( piece_id ) WHERE ( status != 'terminated' );

CREATE OR REPLACE
  FUNCTION egd.init_deal_relations() RETURNS TRIGGER
    LANGUAGE plpgsql
AS $$
BEGIN
  INSERT INTO egd.clients( client_id ) VALUES ( NEW.client_id ) ON CONFLICT DO NOTHING;
  INSERT INTO egd.providers( provider_id ) VALUES ( NEW.provider_id ) ON CONFLICT DO NOTHING;
  IF NEW.piece_id IS NULL THEN
    INSERT INTO egd.pieces( piece_cid, piece_log2_size ) VALUES ( NEW.piece_cid, NEW.claimed_log2_size ) ON CONFLICT DO NOTHING;
    NEW.piece_id = ( SELECT piece_id FROM egd.pieces WHERE piece_cid = NEW.piece_cid );
  END IF;
  RETURN NEW;
 END;
$$;
CREATE OR REPLACE TRIGGER trigger_init_deal_relations
  BEFORE INSERT ON egd.published_deals
  FOR EACH ROW
  EXECUTE PROCEDURE egd.init_deal_relations()
;

CREATE OR REPLACE VIEW egd.known_missized_deals AS (
  SELECT
      pd.*,
      p.piece_log2_size AS proven_log2_size
    FROM egd.published_deals pd, egd.pieces p
  WHERE
    pd.piece_id = p.piece_id
      AND
    pd.claimed_log2_size != p.piece_log2_size
      AND
    (p.piece_meta->'size_proven_correct')::BOOL
);

CREATE TABLE IF NOT EXISTS egd.invalidated_deals (
  deal_id BIGINT NOT NULL UNIQUE REFERENCES egd.published_deals ( deal_id ),
  invalidation_meta JSONB NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS egd.proposals (
  proposal_uuid UUID UNIQUE NOT NULL DEFAULT gen_random_uuid(),
  piece_id BIGINT NOT NULL,

  entry_created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
  signature_obtained TIMESTAMP WITH TIME ZONE,
  proposal_delivered TIMESTAMP WITH TIME ZONE,
  activated_deal_id BIGINT UNIQUE REFERENCES egd.published_deals ( deal_id ),
  proposal_failstamp BIGINT NOT NULL DEFAULT 0 CONSTRAINT proposal_valid_failstamp CHECK ( proposal_failstamp >= 0 ),
  entry_last_updated TIMESTAMP WITH TIME ZONE NOT NULL,

  provider_id INTEGER NOT NULL REFERENCES egd.providers ( provider_id ),
  client_id INTEGER NOT NULL REFERENCES egd.clients ( client_id ),

  start_epoch INTEGER NOT NULL,
  end_epoch INTEGER NOT NULL,

  proxied_log2_size SMALLINT NOT NULL,

  proposal_meta JSONB NOT NULL DEFAULT '{}',

  CONSTRAINT proposal_piece_combo_fkey FOREIGN KEY ( piece_id, proxied_log2_size ) REFERENCES egd.pieces ( piece_id, piece_log2_size ) ON UPDATE CASCADE,
  CONSTRAINT proposal_singleton_pending_piece UNIQUE ( provider_id, piece_id, proposal_failstamp ),
  CONSTRAINT proposal_annotated_failure CHECK ( (proposal_failstamp = 0) = (proposal_meta->'failure' IS NULL) )
);
CREATE OR REPLACE TRIGGER trigger_proposal_update_ts
  BEFORE INSERT OR UPDATE ON egd.proposals
  FOR EACH ROW
  EXECUTE PROCEDURE egd.update_entry_timestamp()
;
CREATE INDEX IF NOT EXISTS proposals_piece_idx ON egd.proposals ( piece_id );
CREATE INDEX IF NOT EXISTS proposals_pending ON egd.proposals ( piece_id, provider_id, client_id ) INCLUDE ( proxied_log2_size ) WHERE ( proposal_failstamp = 0 AND activated_deal_id IS NULL );

-- Used exclusively for the `FilDAG` portion of a `Sources` response and corresponding availability matview
CREATE OR REPLACE VIEW egd.known_fildag_deals_ranked AS (
  SELECT
      piece_id,
      deal_id,
      end_epoch,
      provider_id,
      is_filplus,
      decoded_label AS proposal_label,
      ( ROW_NUMBER() OVER (
        PARTITION BY piece_id, provider_id
        ORDER BY
          is_filplus DESC,
          end_epoch DESC,
          deal_id
      ) ) AS rank
    FROM egd.published_deals pd
    LEFT JOIN egd.invalidated_deals USING ( deal_id )
  WHERE
    invalidated_deals.deal_id IS NULL
      AND
    status = 'active'
      AND
    decoded_label IS NOT NULL
      AND
    decoded_label NOT LIKE 'baga6ea4sea%'
);

CREATE OR REPLACE VIEW egd.clients_datacap_available AS
  SELECT
    c.client_id,
    c.client_address,
    c.tenant_id,
    (
      COALESCE(
        (c.client_meta->'activatable_datacap')::BIGINT,
        0
      )
        -
      COALESCE(
        (
        SELECT SUM( 1::BIGINT << pr.proxied_log2_size )
          FROM egd.proposals pr
        WHERE
          pr.proposal_failstamp = 0
            AND
          pr.activated_deal_id IS NULL
            AND
          pr.client_id = c.client_id
        )::BIGINT,
        0
      )
    ) AS datacap_available
  FROM egd.clients c
  WHERE c.tenant_id IS NOT NULL
  ORDER BY c.tenant_id, datacap_available DESC
;

-- backing virtually all of the functions/materialized views below
-- FIXME: for now it must be a live view as it backs piece_realtime_eligibility(), separate live and tracked parts
CREATE OR REPLACE VIEW egd.known_deals_ranked AS (
  WITH
    cutoff AS MATERIALIZED (
      SELECT egd.replica_expiration_cutoff_epoch() AS epoch
    )
  SELECT
      intra_sp_rank,
      deal_id,
      piece_id,
      provider_id,
      client_id,
      end_epoch,
      state,
      is_filplus,
      proposal_label,
      ( SELECT ARRAY_AGG(DISTINCT id) FROM UNNEST( claimant_ids ) t(id) ) AS claimant_ids -- can not do this below: unsupported in ARRAY_AGG ... OVER
    FROM (
      SELECT
        pub_and_prop.*,
        ( ARRAY_AGG( COALESCE( c.tenant_id, - c.client_id )::INTEGER ) OVER ( PARTITION BY piece_id, provider_id ) ) AS claimant_ids,
        ( ROW_NUMBER() OVER (
          PARTITION BY piece_id, provider_id
          ORDER BY
            ( end_epoch >= cutoff.epoch ) DESC, -- favor not-yet-expiring deals early on
            state DESC,
            ( c.tenant_id IS NOT NULL ) DESC, -- rank any known tenant first
            is_filplus DESC,
            end_epoch DESC,
            deal_id DESC NULLS LAST,
            c.client_id
        ) ) AS intra_sp_rank

      FROM (
        (
          SELECT
              pd.deal_id,
              pd.piece_id,
              pd.provider_id,
              pd.client_id,
              pd.end_epoch,
              ( CASE WHEN pd.status = 'active' THEN 4::"char" ELSE 3::"char" END ) AS state,
              pd.is_filplus,
              pd.decoded_label AS proposal_label
            FROM egd.published_deals pd
            LEFT JOIN egd.invalidated_deals USING ( deal_id )
          WHERE
            invalidated_deals.deal_id IS NULL
              AND
            pd.status != 'terminated'
        )

        UNION ALL

        (
          SELECT
              NULL AS deal_id,
              pr.piece_id,
              pr.provider_id,
              pr.client_id,
              pr.end_epoch,
              ( CASE WHEN pr.proposal_delivered IS NOT NULL THEN 2::"char" ELSE 1::"char" END ) AS state, -- proposed / accepted but not yet chain-published
              true AS is_filplus, -- we do not propose non-filplus
              p.proposal_label
            FROM egd.proposals pr
            JOIN egd.pieces p USING ( piece_id )
            LEFT JOIN egd.published_deals pd
              ON
                pd.piece_id = pr.piece_id
                  AND
                pd.provider_id = pr.provider_id
                  AND
                pd.client_id = pr.client_id
                  AND
                pd.is_filplus
                  AND
                pd.status = 'published'
          WHERE
            pd.piece_id IS NULL
              AND
            pr.proposal_failstamp = 0
              AND
            pr.activated_deal_id IS NULL
        )
      ) pub_and_prop, egd.clients c, cutoff
    WHERE
      pub_and_prop.client_id = c.client_id
  ) fin
);

BEGIN;

DROP FUNCTION IF EXISTS egd.pieces_eligible_head;
DROP FUNCTION IF EXISTS egd.pieces_eligible_full;
DROP FUNCTION IF EXISTS egd.piece_realtime_eligibility;

DROP MATERIALIZED VIEW IF EXISTS egd.mv_pieces_availability;

DROP MATERIALIZED VIEW IF EXISTS egd.mv_overreplicated_total;
DROP MATERIALIZED VIEW IF EXISTS egd.mv_overreplicated_org;
DROP MATERIALIZED VIEW IF EXISTS egd.mv_overreplicated_city;
DROP MATERIALIZED VIEW IF EXISTS egd.mv_overreplicated_country;
DROP MATERIALIZED VIEW IF EXISTS egd.mv_overreplicated_continent;

DROP MATERIALIZED VIEW IF EXISTS egd.mv_replicas_org;
DROP MATERIALIZED VIEW IF EXISTS egd.mv_replicas_city;
DROP MATERIALIZED VIEW IF EXISTS egd.mv_replicas_country;
DROP MATERIALIZED VIEW IF EXISTS egd.mv_replicas_continent;

DROP MATERIALIZED VIEW IF EXISTS egd.mv_deals_prefiltered_for_repcount;
DROP MATERIALIZED VIEW IF EXISTS egd.mv_orglocal_presence;

\timing
;

-- Used exclusively by the 3 functions
CREATE MATERIALIZED VIEW IF NOT EXISTS egd.mv_pieces_availability WITH ( toast_tuple_target = 8160 ) AS (
  SELECT
      ROW_NUMBER() OVER(
        -- MASTER SORT LIVES HERE
        ORDER BY
          ( piece_log2_size < 18 ), -- everything under 256MiB goes to the back of the queue
          http_available DESC,
          coarse_latest_active_end_epoch NULLS LAST,
          piece_cid
      ) AS display_sort,
      *,
      ( http_available OR coarse_latest_active_end_epoch IS NOT NULL ) AS has_sources
    FROM (
      WITH
        pieces_of_interest AS (
          SELECT
              dp.piece_id,
              ARRAY_AGG( DISTINCT( td.tenant_id ) ) AS potential_tenant_ids
            FROM egd.datasets_pieces dp
            JOIN egd.tenants_datasets td USING ( dataset_id )
          GROUP BY dp.piece_id
        )
      SELECT
          p.piece_id,
          (
            SELECT
                egd.coarse_epoch( MAX( kfdr.end_epoch ) )
              FROM egd.known_fildag_deals_ranked kfdr
              WHERE
                kfdr.piece_id = p.piece_id
          ) AS coarse_latest_active_end_epoch,
          p.piece_log2_size,
          ( p.piece_log2_size = 36 ) AS requires_64g_sector,
          false AS http_available,
          p.piece_cid,
          p.proposal_label,
          poi.potential_tenant_ids
        FROM pieces_of_interest poi
        JOIN egd.pieces p USING ( piece_id )
    ) s
) WITH NO DATA;
ALTER MATERIALIZED VIEW egd.mv_pieces_availability ALTER COLUMN piece_cid SET STORAGE MAIN;
ALTER MATERIALIZED VIEW egd.mv_pieces_availability ALTER COLUMN potential_tenant_ids SET STORAGE MAIN;
CREATE UNIQUE INDEX IF NOT EXISTS mv_pieces_availability_key ON egd.mv_pieces_availability ( piece_id );
CREATE INDEX IF NOT EXISTS mv_pieces_availability_standard_sector ON egd.mv_pieces_availability ( piece_id ) WHERE ( NOT requires_64g_sector );
CREATE INDEX IF NOT EXISTS mv_pieces_availability_order ON egd.mv_pieces_availability ( display_sort ) INCLUDE ( piece_id );
CREATE INDEX IF NOT EXISTS mv_pieces_availability_has_sources ON egd.mv_pieces_availability ( piece_id ) WHERE ( has_sources = true );
REFRESH MATERIALIZED VIEW egd.mv_pieces_availability;
ANALYZE egd.mv_pieces_availability;

CREATE OR REPLACE
  FUNCTION egd.piece_realtime_eligibility(
    arg_calling_provider_id INTEGER,
    arg_piece_cid TEXT
  ) RETURNS TABLE (

    piece_id BIGINT,
    proposal_label TEXT,
    piece_size_bytes BIGINT,

    tenant_id SMALLINT,
    client_id_to_use INTEGER,
    client_address_to_use TEXT,
    exclusive_replication BOOL,

    deal_duration_days SMALLINT,
    start_within_hours SMALLINT,

    deal_already_exists BOOL,
    recently_used_start_epoch INTEGER,

    max_in_flight_bytes BIGINT,
    cur_in_flight_bytes BIGINT,

    max_total SMALLINT,
    cur_total SMALLINT,

    max_per_org SMALLINT,
    cur_in_org SMALLINT,

    max_per_city SMALLINT,
    cur_in_city SMALLINT,

    max_per_country SMALLINT,
    cur_in_country SMALLINT,

    max_per_continent SMALLINT,
    cur_in_continent SMALLINT,

    tenant_meta JSONB
  )
LANGUAGE sql PARALLEL RESTRICTED STABLE STRICT AS $$
  WITH
    ctx AS MATERIALIZED (
      SELECT
          p.piece_id,
          ( 1::BIGINT << p.piece_log2_size ) AS piece_size_bytes,
          egd.replica_expiration_cutoff_epoch() AS replica_expiration_cutoff_epoch,
          sp.provider_id,
          sp.org_id,
          sp.city_id,
          sp.country_id,
          sp.continent_id,
          p.proposal_label
        FROM egd.pieces p, egd.providers sp
      WHERE
        p.piece_cid = arg_piece_cid
          AND
        sp.provider_id = arg_calling_provider_id
    ),
    tenant_addresses AS (
      SELECT DISTINCT ON ( cda.tenant_id )
          cda.tenant_id,
          cda.client_id,
          cda.client_address
        FROM egd.clients_datacap_available cda, ctx
      WHERE
        cda.datacap_available >= ctx.piece_size_bytes
      ORDER BY cda.tenant_id, cda.datacap_available
    ),
    available_tenants AS (
      SELECT
          (
            SELECT SUM( datacap_available )
              FROM egd.clients_datacap_available cda
            WHERE cda.tenant_id = t.tenant_id
          ) AS tenant_datacap_available,

          ta.client_id AS client_id_to_use,
          ta.client_address AS client_address_to_use,

          COALESCE(
            (
              SELECT SUM( 1::BIGINT << pr.proxied_log2_size )
                FROM ctx, egd.proposals pr
              WHERE
                pr.provider_id = ctx.provider_id
                  AND
                pr.proposal_failstamp = 0
                  AND
                pr.activated_deal_id IS NULL
                  AND
                pr.client_id IN ( SELECT client_id FROM egd.clients c WHERE c.tenant_id = t.tenant_id )
            )::BIGINT,
            0::BIGINT
          ) AS cur_in_flight_bytes,

          COALESCE( (tp.tenant_provider_meta->'max_in_flight_GiB')::BIGINT,  (t.tenant_meta->'max'->'default_in_flight_GiB')::BIGINT, 1024 )::BIGINT << 30 AS max_in_flight_bytes,

          t.tenant_id,
          ( t.tenant_meta->'deal_params'->'duration_days' )::SMALLINT AS deal_duration_days,
          ( t.tenant_meta->'deal_params'->'start_within_hours' )::SMALLINT AS start_within_hours,

          ( t.tenant_meta->'max'->'total_replicas' )::SMALLINT AS max_total_replicas,
          ( t.tenant_meta->'max'->'per_org' )::SMALLINT AS max_per_org,
          ( t.tenant_meta->'max'->'per_city' )::SMALLINT AS max_per_city,
          ( t.tenant_meta->'max'->'per_country' )::SMALLINT AS max_per_country,
          ( t.tenant_meta->'max'->'per_continent' )::SMALLINT AS max_per_continent,

          COALESCE( ( t.tenant_meta->'max'->'filplus_exclusive' )::BOOL, false ) AS filplus_exclusive,
          COALESCE( ( t.tenant_meta->'max'->'tenant_exclusive' )::BOOL, false ) AS tenant_exclusive,

          t.tenant_meta
        FROM ctx
        JOIN egd.tenants_providers tp USING ( provider_id )
        JOIN egd.tenants t USING ( tenant_id )
        LEFT JOIN tenant_addresses ta USING ( tenant_id )
      WHERE
        NOT COALESCE( ( tp.tenant_provider_meta->'inactivated' )::BOOL, false )
          AND
        t.tenant_id IN (
          SELECT UNNEST( pa.potential_tenant_ids )
            FROM egd.mv_pieces_availability pa
          WHERE pa.piece_id = ctx.piece_id
        )
    ),
    eligibility AS MATERIALIZED (
      SELECT
          ctx.piece_id,
          ctx.proposal_label,
          ctx.piece_size_bytes,

          at.tenant_id,
          at.tenant_datacap_available,
          at.client_id_to_use,
          at.client_address_to_use,
          at.tenant_exclusive,

          at.deal_duration_days,
          at.start_within_hours,

          (
            SELECT MAX( start_epoch )
              FROM egd.proposals pr
              JOIN egd.clients c USING ( client_id )
            WHERE
              pr.piece_id = ctx.piece_id
                AND
              pr.provider_id = ctx.provider_id
                AND
              ( at.tenant_id = c.tenant_id OR NOT at.tenant_exclusive )
          ) AS previous_start_epoch,

          EXISTS (
            SELECT 42
              FROM egd.known_deals_ranked kdr
            WHERE
              kdr.piece_id = ctx.piece_id
                AND
              kdr.provider_id = ctx.provider_id
                AND
              kdr.end_epoch >= ctx.replica_expiration_cutoff_epoch
                AND
              ( kdr.is_filplus OR NOT at.filplus_exclusive )
                AND
              ( at.tenant_id = ANY ( kdr.claimant_ids ) OR NOT at.tenant_exclusive  )
          ) AS deal_already_exists,

          at.max_in_flight_bytes,
          at.cur_in_flight_bytes,

          at.max_total_replicas AS max_total,
          (
            SELECT COUNT(DISTINCT( kdr.provider_id ))::SMALLINT
              FROM egd.known_deals_ranked kdr
            WHERE
              kdr.piece_id = ctx.piece_id
                AND
              kdr.end_epoch >= ctx.replica_expiration_cutoff_epoch
                AND
              ( kdr.is_filplus OR NOT at.filplus_exclusive )
                AND
              ( at.tenant_id = ANY ( kdr.claimant_ids ) OR NOT at.tenant_exclusive )
          ) AS cur_total,

          -- next 4 are generated from a template
          /*

          perl -E '
            @spatial_types = qw( org city country continent );
            say join "\n",

            ( map { "
          at.max_per_${_},
          (
            SELECT COUNT(DISTINCT( kdr.provider_id ))::SMALLINT
              FROM egd.known_deals_ranked kdr, egd.providers p
            WHERE
              kdr.piece_id = ctx.piece_id
                AND
              kdr.end_epoch >= ctx.replica_expiration_cutoff_epoch
                AND
              kdr.provider_id = p.provider_id
                AND
              p.${_}_id = ctx.${_}_id
                AND
              ( kdr.is_filplus OR NOT at.filplus_exclusive )
                AND
              ( at.tenant_id = ANY ( kdr.claimant_ids ) OR NOT at.tenant_exclusive  )
          ) AS cur_in_${_}," }
            @spatial_types )

          ' | pbcopy

          */

          -- BEGIN SQLGEN

          at.max_per_org,
          (
            SELECT COUNT(DISTINCT( kdr.provider_id ))::SMALLINT
              FROM egd.known_deals_ranked kdr, egd.providers p
            WHERE
              kdr.piece_id = ctx.piece_id
                AND
              kdr.end_epoch >= ctx.replica_expiration_cutoff_epoch
                AND
              kdr.provider_id = p.provider_id
                AND
              p.org_id = ctx.org_id
                AND
              ( kdr.is_filplus OR NOT at.filplus_exclusive )
                AND
              ( at.tenant_id = ANY ( kdr.claimant_ids ) OR NOT at.tenant_exclusive  )
          ) AS cur_in_org,

          at.max_per_city,
          (
            SELECT COUNT(DISTINCT( kdr.provider_id ))::SMALLINT
              FROM egd.known_deals_ranked kdr, egd.providers p
            WHERE
              kdr.piece_id = ctx.piece_id
                AND
              kdr.end_epoch >= ctx.replica_expiration_cutoff_epoch
                AND
              kdr.provider_id = p.provider_id
                AND
              p.city_id = ctx.city_id
                AND
              ( kdr.is_filplus OR NOT at.filplus_exclusive )
                AND
              ( at.tenant_id = ANY ( kdr.claimant_ids ) OR NOT at.tenant_exclusive  )
          ) AS cur_in_city,

          at.max_per_country,
          (
            SELECT COUNT(DISTINCT( kdr.provider_id ))::SMALLINT
              FROM egd.known_deals_ranked kdr, egd.providers p
            WHERE
              kdr.piece_id = ctx.piece_id
                AND
              kdr.end_epoch >= ctx.replica_expiration_cutoff_epoch
                AND
              kdr.provider_id = p.provider_id
                AND
              p.country_id = ctx.country_id
                AND
              ( kdr.is_filplus OR NOT at.filplus_exclusive )
                AND
              ( at.tenant_id = ANY ( kdr.claimant_ids ) OR NOT at.tenant_exclusive  )
          ) AS cur_in_country,

          at.max_per_continent,
          (
            SELECT COUNT(DISTINCT( kdr.provider_id ))::SMALLINT
              FROM egd.known_deals_ranked kdr, egd.providers p
            WHERE
              kdr.piece_id = ctx.piece_id
                AND
              kdr.end_epoch >= ctx.replica_expiration_cutoff_epoch
                AND
              kdr.provider_id = p.provider_id
                AND
              p.continent_id = ctx.continent_id
                AND
              ( kdr.is_filplus OR NOT at.filplus_exclusive )
                AND
              ( at.tenant_id = ANY ( kdr.claimant_ids ) OR NOT at.tenant_exclusive  )
          ) AS cur_in_continent,
          -- END SQLGEN

          at.tenant_meta

        FROM ctx, available_tenants at

    )
  SELECT
      piece_id,
      proposal_label,
      piece_size_bytes,
      tenant_id,
      client_id_to_use,
      client_address_to_use,
      tenant_exclusive,
      deal_duration_days, start_within_hours,
      deal_already_exists,
      CASE WHEN previous_start_epoch > egd.proposal_deduplication_recent_cutoff_epoch()
        THEN previous_start_epoch
        ELSE NULL::INTEGER
      END AS recently_used_start_epoch,
      max_in_flight_bytes, cur_in_flight_bytes,
      max_total, cur_total,
      max_per_org, cur_in_org,
      max_per_city, cur_in_city,
      max_per_country, cur_in_country,
      max_per_continent, cur_in_continent,
      tenant_meta JSONB
    FROM eligibility
  ORDER BY
    -- eligible 1st
    (
      NOT deal_already_exists
        AND
      client_id_to_use IS NOT NULL
        AND
      cur_in_flight_bytes < max_in_flight_bytes
        AND
      cur_total < max_total
        AND
      cur_in_org < max_per_org
        AND
      cur_in_city < max_per_city
        AND
      cur_in_country < max_per_country
        AND
      cur_in_continent < max_per_continent
    ) DESC,
    tenant_exclusive DESC, -- exclusive 1st
    tenant_datacap_available DESC
$$;


-- internal materialization, not used by the app directly
CREATE MATERIALIZED VIEW IF NOT EXISTS egd.mv_deals_prefiltered_for_repcount AS
  WITH
    cutoff AS MATERIALIZED (
      SELECT egd.replica_expiration_cutoff_epoch() AS epoch
    ),
    filtered AS (
      SELECT
          kdr.piece_id,
          kdr.provider_id,
          kdr.is_filplus,
          kdr.claimant_ids
        FROM egd.known_deals_ranked kdr, cutoff
      WHERE
        kdr.piece_id IN (  -- restrict to only pieces we care about
          SELECT dp.piece_id
            FROM egd.datasets_pieces dp
            JOIN egd.tenants_datasets td USING ( dataset_id )
        )
          AND
        kdr.end_epoch >= cutoff.epoch
          AND
        kdr.state > 1::"char"
          AND
        kdr.intra_sp_rank = 1 -- this is safe, since the rank is based on the above 2 conditions
    )

  (
    SELECT
        piece_id,
        provider_id,
        is_filplus,
        UNNEST( claimant_ids ) AS claimant_id
      FROM filtered

  UNION ALL

    SELECT
        piece_id,
        provider_id,
        is_filplus,
        0 AS claimant_id
      FROM filtered
  )
  ORDER BY provider_id, claimant_id, piece_id, is_filplus
;
CREATE UNIQUE INDEX IF NOT EXISTS mv_deals_prefiltered_for_repcount_key ON egd.mv_deals_prefiltered_for_repcount ( provider_id, claimant_id, piece_id, is_filplus );
ANALYZE egd.mv_deals_prefiltered_for_repcount;

-- next 2*4 are generated from a template
-- the continent one doubles as a `totals` table, with continent_id being NULL
/*

perl -E '
  @spatial_types = qw( continent country city org );
  say join "\n",

    ( map { "
CREATE MATERIALIZED VIEW IF NOT EXISTS egd.mv_replicas_${_} AS
  (
    (
      SELECT
          piece_id,
          claimant_id,
          ${_}_id,
          ( COUNT(*) FILTER ( WHERE is_filplus ) )::SMALLINT AS replicas_filplus,
          ( COUNT(*) )::SMALLINT AS replicas_any
        FROM egd.mv_deals_prefiltered_for_repcount
        JOIN egd.providers USING ( provider_id )
      WHERE claimant_id != 0
      GROUP BY
        piece_id, ${_}_id, claimant_id
    )
      UNION ALL
    (
      SELECT
          piece_id,
          NULL::INTEGER,
          ${_}_id,
          ( COUNT(*) FILTER ( WHERE is_filplus ) )::SMALLINT AS replicas_filplus,
          ( COUNT(*) )::SMALLINT AS replicas_any
        FROM egd.mv_deals_prefiltered_for_repcount
        JOIN egd.providers USING ( provider_id )
      WHERE claimant_id = 0
      GROUP BY GROUPING SETS (
        @{[ ( $_ eq q{continent} ) ? q{( piece_id ),} : q{} ]}
        ( piece_id, ${_}_id )
      )
    )
  ) ORDER BY piece_id, ${_}_id, claimant_id
;
CREATE UNIQUE INDEX IF NOT EXISTS mv_replicas_${_}_idx ON egd.mv_replicas_${_} ( piece_id, ${_}_id, claimant_id );
ANALYZE egd.mv_replicas_${_};

CREATE MATERIALIZED VIEW IF NOT EXISTS egd.mv_overreplicated_${_} AS
  SELECT DISTINCT
      r.piece_id,
      t.tenant_id,
      r.${_}_id
    FROM egd.mv_replicas_${_} r
    JOIN egd.datasets_pieces USING ( piece_id )
    JOIN egd.tenants_datasets USING ( dataset_id )
    JOIN egd.tenants t USING ( tenant_id )
  WHERE
    r.${_}_id > 0

      AND

    (
      (
        r.claimant_id = t.tenant_id
          AND
        ( t.tenant_meta->\x{27}max\x{27}->\x{27}tenant_exclusive\x{27} )::BOOL
      )
        OR
      (
        r.claimant_id IS NULL
          AND
        NOT COALESCE( ( t.tenant_meta->\x{27}max\x{27}->\x{27}tenant_exclusive\x{27} )::BOOL, false )
      )
    )

      AND

    (
      (
        ( t.tenant_meta->\x{27}max\x{27}->\x{27}filplus_exclusive\x{27} )::BOOL
          AND
        ( t.tenant_meta->\x{27}max\x{27}->\x{27}per_${_}\x{27} )::SMALLINT <= r.replicas_filplus
      )
        OR
      (
        NOT COALESCE( ( t.tenant_meta->\x{27}max\x{27}->\x{27}filplus_exclusive\x{27} )::BOOL, false )
          AND
        ( t.tenant_meta->\x{27}max\x{27}->\x{27}per_${_}\x{27} )::SMALLINT <= r.replicas_any
      )
    )
  ORDER BY piece_id, ${_}_id, tenant_id
;
CREATE UNIQUE INDEX IF NOT EXISTS mv_overreplicated_${_}_idx ON egd.mv_overreplicated_${_} ( piece_id, ${_}_id, tenant_id );
ANALYZE egd.mv_overreplicated_${_};
" }
  @spatial_types )

' | pbcopy

*/

-- BEGIN SQLGEN

CREATE MATERIALIZED VIEW IF NOT EXISTS egd.mv_replicas_continent AS
  (
    (
      SELECT
          piece_id,
          claimant_id,
          continent_id,
          ( COUNT(*) FILTER ( WHERE is_filplus ) )::SMALLINT AS replicas_filplus,
          ( COUNT(*) )::SMALLINT AS replicas_any
        FROM egd.mv_deals_prefiltered_for_repcount
        JOIN egd.providers USING ( provider_id )
      WHERE claimant_id != 0
      GROUP BY
        piece_id, continent_id, claimant_id
    )
      UNION ALL
    (
      SELECT
          piece_id,
          NULL::INTEGER,
          continent_id,
          ( COUNT(*) FILTER ( WHERE is_filplus ) )::SMALLINT AS replicas_filplus,
          ( COUNT(*) )::SMALLINT AS replicas_any
        FROM egd.mv_deals_prefiltered_for_repcount
        JOIN egd.providers USING ( provider_id )
      WHERE claimant_id = 0
      GROUP BY GROUPING SETS (
        ( piece_id ),
        ( piece_id, continent_id )
      )
    )
  ) ORDER BY piece_id, continent_id, claimant_id
;
CREATE UNIQUE INDEX IF NOT EXISTS mv_replicas_continent_idx ON egd.mv_replicas_continent ( piece_id, continent_id, claimant_id );
ANALYZE egd.mv_replicas_continent;

CREATE MATERIALIZED VIEW IF NOT EXISTS egd.mv_overreplicated_continent AS
  SELECT DISTINCT
      r.piece_id,
      t.tenant_id,
      r.continent_id
    FROM egd.mv_replicas_continent r
    JOIN egd.datasets_pieces USING ( piece_id )
    JOIN egd.tenants_datasets USING ( dataset_id )
    JOIN egd.tenants t USING ( tenant_id )
  WHERE
    r.continent_id > 0

      AND

    (
      (
        r.claimant_id = t.tenant_id
          AND
        ( t.tenant_meta->'max'->'tenant_exclusive' )::BOOL
      )
        OR
      (
        r.claimant_id IS NULL
          AND
        NOT COALESCE( ( t.tenant_meta->'max'->'tenant_exclusive' )::BOOL, false )
      )
    )

      AND

    (
      (
        ( t.tenant_meta->'max'->'filplus_exclusive' )::BOOL
          AND
        ( t.tenant_meta->'max'->'per_continent' )::SMALLINT <= r.replicas_filplus
      )
        OR
      (
        NOT COALESCE( ( t.tenant_meta->'max'->'filplus_exclusive' )::BOOL, false )
          AND
        ( t.tenant_meta->'max'->'per_continent' )::SMALLINT <= r.replicas_any
      )
    )
  ORDER BY piece_id, continent_id, tenant_id
;
CREATE UNIQUE INDEX IF NOT EXISTS mv_overreplicated_continent_idx ON egd.mv_overreplicated_continent ( piece_id, continent_id, tenant_id );
ANALYZE egd.mv_overreplicated_continent;


CREATE MATERIALIZED VIEW IF NOT EXISTS egd.mv_replicas_country AS
  (
    (
      SELECT
          piece_id,
          claimant_id,
          country_id,
          ( COUNT(*) FILTER ( WHERE is_filplus ) )::SMALLINT AS replicas_filplus,
          ( COUNT(*) )::SMALLINT AS replicas_any
        FROM egd.mv_deals_prefiltered_for_repcount
        JOIN egd.providers USING ( provider_id )
      WHERE claimant_id != 0
      GROUP BY
        piece_id, country_id, claimant_id
    )
      UNION ALL
    (
      SELECT
          piece_id,
          NULL::INTEGER,
          country_id,
          ( COUNT(*) FILTER ( WHERE is_filplus ) )::SMALLINT AS replicas_filplus,
          ( COUNT(*) )::SMALLINT AS replicas_any
        FROM egd.mv_deals_prefiltered_for_repcount
        JOIN egd.providers USING ( provider_id )
      WHERE claimant_id = 0
      GROUP BY GROUPING SETS (

        ( piece_id, country_id )
      )
    )
  ) ORDER BY piece_id, country_id, claimant_id
;
CREATE UNIQUE INDEX IF NOT EXISTS mv_replicas_country_idx ON egd.mv_replicas_country ( piece_id, country_id, claimant_id );
ANALYZE egd.mv_replicas_country;

CREATE MATERIALIZED VIEW IF NOT EXISTS egd.mv_overreplicated_country AS
  SELECT DISTINCT
      r.piece_id,
      t.tenant_id,
      r.country_id
    FROM egd.mv_replicas_country r
    JOIN egd.datasets_pieces USING ( piece_id )
    JOIN egd.tenants_datasets USING ( dataset_id )
    JOIN egd.tenants t USING ( tenant_id )
  WHERE
    r.country_id > 0

      AND

    (
      (
        r.claimant_id = t.tenant_id
          AND
        ( t.tenant_meta->'max'->'tenant_exclusive' )::BOOL
      )
        OR
      (
        r.claimant_id IS NULL
          AND
        NOT COALESCE( ( t.tenant_meta->'max'->'tenant_exclusive' )::BOOL, false )
      )
    )

      AND

    (
      (
        ( t.tenant_meta->'max'->'filplus_exclusive' )::BOOL
          AND
        ( t.tenant_meta->'max'->'per_country' )::SMALLINT <= r.replicas_filplus
      )
        OR
      (
        NOT COALESCE( ( t.tenant_meta->'max'->'filplus_exclusive' )::BOOL, false )
          AND
        ( t.tenant_meta->'max'->'per_country' )::SMALLINT <= r.replicas_any
      )
    )
  ORDER BY piece_id, country_id, tenant_id
;
CREATE UNIQUE INDEX IF NOT EXISTS mv_overreplicated_country_idx ON egd.mv_overreplicated_country ( piece_id, country_id, tenant_id );
ANALYZE egd.mv_overreplicated_country;


CREATE MATERIALIZED VIEW IF NOT EXISTS egd.mv_replicas_city AS
  (
    (
      SELECT
          piece_id,
          claimant_id,
          city_id,
          ( COUNT(*) FILTER ( WHERE is_filplus ) )::SMALLINT AS replicas_filplus,
          ( COUNT(*) )::SMALLINT AS replicas_any
        FROM egd.mv_deals_prefiltered_for_repcount
        JOIN egd.providers USING ( provider_id )
      WHERE claimant_id != 0
      GROUP BY
        piece_id, city_id, claimant_id
    )
      UNION ALL
    (
      SELECT
          piece_id,
          NULL::INTEGER,
          city_id,
          ( COUNT(*) FILTER ( WHERE is_filplus ) )::SMALLINT AS replicas_filplus,
          ( COUNT(*) )::SMALLINT AS replicas_any
        FROM egd.mv_deals_prefiltered_for_repcount
        JOIN egd.providers USING ( provider_id )
      WHERE claimant_id = 0
      GROUP BY GROUPING SETS (

        ( piece_id, city_id )
      )
    )
  ) ORDER BY piece_id, city_id, claimant_id
;
CREATE UNIQUE INDEX IF NOT EXISTS mv_replicas_city_idx ON egd.mv_replicas_city ( piece_id, city_id, claimant_id );
ANALYZE egd.mv_replicas_city;

CREATE MATERIALIZED VIEW IF NOT EXISTS egd.mv_overreplicated_city AS
  SELECT DISTINCT
      r.piece_id,
      t.tenant_id,
      r.city_id
    FROM egd.mv_replicas_city r
    JOIN egd.datasets_pieces USING ( piece_id )
    JOIN egd.tenants_datasets USING ( dataset_id )
    JOIN egd.tenants t USING ( tenant_id )
  WHERE
    r.city_id > 0

      AND

    (
      (
        r.claimant_id = t.tenant_id
          AND
        ( t.tenant_meta->'max'->'tenant_exclusive' )::BOOL
      )
        OR
      (
        r.claimant_id IS NULL
          AND
        NOT COALESCE( ( t.tenant_meta->'max'->'tenant_exclusive' )::BOOL, false )
      )
    )

      AND

    (
      (
        ( t.tenant_meta->'max'->'filplus_exclusive' )::BOOL
          AND
        ( t.tenant_meta->'max'->'per_city' )::SMALLINT <= r.replicas_filplus
      )
        OR
      (
        NOT COALESCE( ( t.tenant_meta->'max'->'filplus_exclusive' )::BOOL, false )
          AND
        ( t.tenant_meta->'max'->'per_city' )::SMALLINT <= r.replicas_any
      )
    )
  ORDER BY piece_id, city_id, tenant_id
;
CREATE UNIQUE INDEX IF NOT EXISTS mv_overreplicated_city_idx ON egd.mv_overreplicated_city ( piece_id, city_id, tenant_id );
ANALYZE egd.mv_overreplicated_city;


CREATE MATERIALIZED VIEW IF NOT EXISTS egd.mv_replicas_org AS
  (
    (
      SELECT
          piece_id,
          claimant_id,
          org_id,
          ( COUNT(*) FILTER ( WHERE is_filplus ) )::SMALLINT AS replicas_filplus,
          ( COUNT(*) )::SMALLINT AS replicas_any
        FROM egd.mv_deals_prefiltered_for_repcount
        JOIN egd.providers USING ( provider_id )
      WHERE claimant_id != 0
      GROUP BY
        piece_id, org_id, claimant_id
    )
      UNION ALL
    (
      SELECT
          piece_id,
          NULL::INTEGER,
          org_id,
          ( COUNT(*) FILTER ( WHERE is_filplus ) )::SMALLINT AS replicas_filplus,
          ( COUNT(*) )::SMALLINT AS replicas_any
        FROM egd.mv_deals_prefiltered_for_repcount
        JOIN egd.providers USING ( provider_id )
      WHERE claimant_id = 0
      GROUP BY GROUPING SETS (

        ( piece_id, org_id )
      )
    )
  ) ORDER BY piece_id, org_id, claimant_id
;
CREATE UNIQUE INDEX IF NOT EXISTS mv_replicas_org_idx ON egd.mv_replicas_org ( piece_id, org_id, claimant_id );
ANALYZE egd.mv_replicas_org;

CREATE MATERIALIZED VIEW IF NOT EXISTS egd.mv_overreplicated_org AS
  SELECT DISTINCT
      r.piece_id,
      t.tenant_id,
      r.org_id
    FROM egd.mv_replicas_org r
    JOIN egd.datasets_pieces USING ( piece_id )
    JOIN egd.tenants_datasets USING ( dataset_id )
    JOIN egd.tenants t USING ( tenant_id )
  WHERE
    r.org_id > 0

      AND

    (
      (
        r.claimant_id = t.tenant_id
          AND
        ( t.tenant_meta->'max'->'tenant_exclusive' )::BOOL
      )
        OR
      (
        r.claimant_id IS NULL
          AND
        NOT COALESCE( ( t.tenant_meta->'max'->'tenant_exclusive' )::BOOL, false )
      )
    )

      AND

    (
      (
        ( t.tenant_meta->'max'->'filplus_exclusive' )::BOOL
          AND
        ( t.tenant_meta->'max'->'per_org' )::SMALLINT <= r.replicas_filplus
      )
        OR
      (
        NOT COALESCE( ( t.tenant_meta->'max'->'filplus_exclusive' )::BOOL, false )
          AND
        ( t.tenant_meta->'max'->'per_org' )::SMALLINT <= r.replicas_any
      )
    )
  ORDER BY piece_id, org_id, tenant_id
;
CREATE UNIQUE INDEX IF NOT EXISTS mv_overreplicated_org_idx ON egd.mv_overreplicated_org ( piece_id, org_id, tenant_id );
ANALYZE egd.mv_overreplicated_org;

-- END SQLGEN

CREATE MATERIALIZED VIEW IF NOT EXISTS egd.mv_overreplicated_total AS

  SELECT DISTINCT
      r.piece_id,
      t.tenant_id
    FROM egd.mv_replicas_continent r
    JOIN egd.datasets_pieces USING ( piece_id )
    JOIN egd.tenants_datasets USING ( dataset_id )
    JOIN egd.tenants t USING ( tenant_id )
  WHERE

    r.continent_id IS NULL

      AND

    (
      (
        r.claimant_id = t.tenant_id
          AND
        ( t.tenant_meta->'max'->'tenant_exclusive' )::BOOL
      )
        OR
      (
        r.claimant_id IS NULL
          AND
        NOT COALESCE( ( t.tenant_meta->'max'->'tenant_exclusive' )::BOOL, false )
      )
    )

      AND

    (
      (
        ( t.tenant_meta->'max'->'filplus_exclusive' )::BOOL
          AND
        ( t.tenant_meta->'max'->'total_replicas' )::SMALLINT <= r.replicas_filplus
      )
        OR
      (
        NOT COALESCE( ( t.tenant_meta->'max'->'filplus_exclusive' )::BOOL, false )
          AND
        ( t.tenant_meta->'max'->'total_replicas' )::SMALLINT <= r.replicas_any
      )
    )
  ORDER BY piece_id, tenant_id
;
CREATE UNIQUE INDEX IF NOT EXISTS mv_overreplicated_total_idx ON egd.mv_overreplicated_total ( piece_id, tenant_id );
ANALYZE egd.mv_overreplicated_total;


-- enable the orglocal variant below
-- this is *distinct* from mv_replicas_org: it lists deals in any live state on chain
CREATE MATERIALIZED VIEW IF NOT EXISTS egd.mv_orglocal_presence AS
  SELECT DISTINCT
      pd.piece_id,
      p.org_id
    FROM egd.published_deals pd
    JOIN egd.providers p USING ( provider_id )
    LEFT JOIN egd.invalidated_deals id USING ( deal_id )
  WHERE
    p.org_id != 0
      AND
    pd.status != 'terminated'
      AND
    id.deal_id IS NULL
  ORDER BY pd.piece_id, p.org_id
;
CREATE UNIQUE INDEX IF NOT EXISTS mv_orglocal_presence_key ON egd.mv_orglocal_presence ( piece_id, org_id );
ANALYZE egd.mv_orglocal_presence;


/*
Templated function in 2 variants:
- egd.pieces_eligible_head(...) Inline (lateral) overreplication-evaluator, allows for linear-slowdown LIMIT
- egd.pieces_eligible_full(...) Pre-calculated overreplication-evaluator, steep up-front cost, sublinear-slowdown LIMIT on larger set

Regenerate via:

perl -E '

  my $no_overrep_cond =
      "
        et.tenant_id = ANY( pa.potential_tenant_ids )

          AND

        NOT EXISTS (
          SELECT 42
            FROM egd.mv_overreplicated_total o_total
          WHERE
            o_total.piece_id = pa.piece_id
              AND
            o_total.tenant_id = et.tenant_id
        )

          AND

        @{[
          join \"\n
          AND\n\", map {
        \"
        NOT EXISTS (
          SELECT 42
            FROM egd.mv_overreplicated_${_} o_${_}
          WHERE
            o_${_}.piece_id = pa.piece_id
              AND
            o_${_}.tenant_id = et.tenant_id
              AND
            o_${_}.${_}_id = sp.${_}_id
        )\"
      } qw( org city country continent )
  ]}";

  my $parts = {

    head => {
      COND => "
    --
    -- there are enabled tenants
    -- ( we *MUST* check a COUNT(*), otherwise the lateral executes for *everything* and only then returns 0 )
    ( SELECT COUNT(*) FROM enabled_tenants ) > 0

      AND

    -- there are claiming tenants
    claiming_tenants.tenant_ids IS NOT NULL
",

      FROM => ", LATERAL (
      SELECT
          ARRAY_AGG( et.tenant_id ) AS tenant_ids
        FROM enabled_tenants et

      WHERE

$no_overrep_cond
      ) claiming_tenants",

      CTE => "",
    },

    full => {
      COND => "
    --
    -- there is a list of interested+enabled tenants
    claiming_tenants.piece_id = pa.piece_id
",

      FROM => ", claiming_tenants",

      CTE => ",
    claiming_tenants AS (
      SELECT
          piece_id,
          ARRAY_AGG( et.tenant_id ) AS tenant_ids
        FROM egd.mv_pieces_availability pa, sp, enabled_tenants et

      WHERE
        $no_overrep_cond
      GROUP BY piece_id
    )",
    },
  };

  say join "\n", map {
    "
CREATE OR REPLACE
  FUNCTION egd.pieces_eligible_${_}(
    arg_calling_provider_id INTEGER,
    arg_limit INTEGER,
    arg_only_tenant_id SMALLINT, -- use 0 for ~any~
    arg_include_sourceless BOOL,
    arg_only_orglocal BOOL
  ) RETURNS TABLE (
    piece_id BIGINT,
    piece_log2_size SMALLINT,
    has_sources_http BOOL,
    has_sources_fil_active BOOL,
    piece_cid TEXT,
    tenant_ids SMALLINT[]
  )
LANGUAGE sql PARALLEL RESTRICTED STABLE STRICT AS \$\$

  WITH
    sp AS MATERIALIZED (
      SELECT
          sp.org_id,
          sp.city_id,
          sp.country_id,
          sp.continent_id,
          ( COALESCE( (spi.info->\x{27}sector_log2_size\x{27})::BIGINT, 0 ) >= 36 ) AS can_seal_64g_sectors
        FROM egd.providers sp
        LEFT JOIN egd.providers_info spi USING ( provider_id )
      WHERE
        sp.provider_id = arg_calling_provider_id
    ),
    enabled_tenants AS MATERIALIZED (
      SELECT tp.tenant_id
        FROM egd.tenants_providers tp
      WHERE
        tp.provider_id = arg_calling_provider_id
          AND
        NOT COALESCE( ( tp.tenant_provider_meta->\x{27}inactivated\x{27} )::BOOL, false )
          AND
        ( arg_only_tenant_id = 0 OR arg_only_tenant_id = tp.tenant_id )
    )
    $parts->{$_}{CTE}

  SELECT
      pa.piece_id,
      pa.piece_log2_size,
      pa.http_available AS has_sources_http,
      ( pa.coarse_latest_active_end_epoch IS NOT NULL ) AS has_sources_fil_active,
      pa.piece_cid,
      claiming_tenants.tenant_ids
    FROM egd.mv_pieces_availability pa, sp $parts->{$_}{FROM}

  WHERE $parts->{$_}{COND}
      AND

    --
    -- can we seal it ourselves?
    (
      sp.can_seal_64g_sectors
        OR
      NOT pa.requires_64g_sector
    )

      AND

    --
    -- sourcelessness
    (
      arg_include_sourceless
        OR
      pa.has_sources = true
    )

      AND

    --
    -- orglocal only
    (
      NOT arg_only_orglocal
        OR
      EXISTS (
        SELECT 42
          FROM egd.mv_orglocal_presence op
        WHERE
          op.piece_id = pa.piece_id
            AND
          op.org_id = sp.org_id
      )
    )

      AND

    --
    -- exclude my own known/in-flight
    NOT EXISTS (
      SELECT 42
        FROM egd.mv_deals_prefiltered_for_repcount dpfr
      WHERE
        dpfr.piece_id = pa.piece_id
          AND
        dpfr.provider_id = arg_calling_provider_id
    )

      AND

    --
    -- exclude my own pre-flight
    NOT EXISTS (
      SELECT 42
        FROM egd.proposals pr
      WHERE
        pr.piece_id = pa.piece_id
          AND
        pr.provider_id = arg_calling_provider_id
          AND
        pr.proposal_failstamp = 0
          AND
        pr.activated_deal_id IS NULL
    )

  ORDER BY display_sort

  LIMIT arg_limit
\$\$;
"

  } qw( head full )
' | pbcopy

*/

-- BEGIN SQLGEN

CREATE OR REPLACE
  FUNCTION egd.pieces_eligible_head(
    arg_calling_provider_id INTEGER,
    arg_limit INTEGER,
    arg_only_tenant_id SMALLINT, -- use 0 for ~any~
    arg_include_sourceless BOOL,
    arg_only_orglocal BOOL
  ) RETURNS TABLE (
    piece_id BIGINT,
    piece_log2_size SMALLINT,
    has_sources_http BOOL,
    has_sources_fil_active BOOL,
    piece_cid TEXT,
    tenant_ids SMALLINT[]
  )
LANGUAGE sql PARALLEL RESTRICTED STABLE STRICT AS $$

  WITH
    sp AS MATERIALIZED (
      SELECT
          sp.org_id,
          sp.city_id,
          sp.country_id,
          sp.continent_id,
          ( COALESCE( (spi.info->'sector_log2_size')::BIGINT, 0 ) >= 36 ) AS can_seal_64g_sectors
        FROM egd.providers sp
        LEFT JOIN egd.providers_info spi USING ( provider_id )
      WHERE
        sp.provider_id = arg_calling_provider_id
    ),
    enabled_tenants AS MATERIALIZED (
      SELECT tp.tenant_id
        FROM egd.tenants_providers tp
      WHERE
        tp.provider_id = arg_calling_provider_id
          AND
        NOT COALESCE( ( tp.tenant_provider_meta->'inactivated' )::BOOL, false )
          AND
        ( arg_only_tenant_id = 0 OR arg_only_tenant_id = tp.tenant_id )
    )


  SELECT
      pa.piece_id,
      pa.piece_log2_size,
      pa.http_available AS has_sources_http,
      ( pa.coarse_latest_active_end_epoch IS NOT NULL ) AS has_sources_fil_active,
      pa.piece_cid,
      claiming_tenants.tenant_ids
    FROM egd.mv_pieces_availability pa, sp , LATERAL (
      SELECT
          ARRAY_AGG( et.tenant_id ) AS tenant_ids
        FROM enabled_tenants et

      WHERE


        et.tenant_id = ANY( pa.potential_tenant_ids )

          AND

        NOT EXISTS (
          SELECT 42
            FROM egd.mv_overreplicated_total o_total
          WHERE
            o_total.piece_id = pa.piece_id
              AND
            o_total.tenant_id = et.tenant_id
        )

          AND


        NOT EXISTS (
          SELECT 42
            FROM egd.mv_overreplicated_org o_org
          WHERE
            o_org.piece_id = pa.piece_id
              AND
            o_org.tenant_id = et.tenant_id
              AND
            o_org.org_id = sp.org_id
        )

          AND

        NOT EXISTS (
          SELECT 42
            FROM egd.mv_overreplicated_city o_city
          WHERE
            o_city.piece_id = pa.piece_id
              AND
            o_city.tenant_id = et.tenant_id
              AND
            o_city.city_id = sp.city_id
        )

          AND

        NOT EXISTS (
          SELECT 42
            FROM egd.mv_overreplicated_country o_country
          WHERE
            o_country.piece_id = pa.piece_id
              AND
            o_country.tenant_id = et.tenant_id
              AND
            o_country.country_id = sp.country_id
        )

          AND

        NOT EXISTS (
          SELECT 42
            FROM egd.mv_overreplicated_continent o_continent
          WHERE
            o_continent.piece_id = pa.piece_id
              AND
            o_continent.tenant_id = et.tenant_id
              AND
            o_continent.continent_id = sp.continent_id
        )
      ) claiming_tenants

  WHERE
    --
    -- there are enabled tenants
    -- ( we *MUST* check a COUNT(*), otherwise the lateral executes for *everything* and only then returns 0 )
    ( SELECT COUNT(*) FROM enabled_tenants ) > 0

      AND

    -- there are claiming tenants
    claiming_tenants.tenant_ids IS NOT NULL

      AND

    --
    -- can we seal it ourselves?
    (
      sp.can_seal_64g_sectors
        OR
      NOT pa.requires_64g_sector
    )

      AND

    --
    -- sourcelessness
    (
      arg_include_sourceless
        OR
      pa.has_sources = true
    )

      AND

    --
    -- orglocal only
    (
      NOT arg_only_orglocal
        OR
      EXISTS (
        SELECT 42
          FROM egd.mv_orglocal_presence op
        WHERE
          op.piece_id = pa.piece_id
            AND
          op.org_id = sp.org_id
      )
    )

      AND

    --
    -- exclude my own known/in-flight
    NOT EXISTS (
      SELECT 42
        FROM egd.mv_deals_prefiltered_for_repcount dpfr
      WHERE
        dpfr.piece_id = pa.piece_id
          AND
        dpfr.provider_id = arg_calling_provider_id
    )

      AND

    --
    -- exclude my own pre-flight
    NOT EXISTS (
      SELECT 42
        FROM egd.proposals pr
      WHERE
        pr.piece_id = pa.piece_id
          AND
        pr.provider_id = arg_calling_provider_id
          AND
        pr.proposal_failstamp = 0
          AND
        pr.activated_deal_id IS NULL
    )

  ORDER BY display_sort

  LIMIT arg_limit
$$;


CREATE OR REPLACE
  FUNCTION egd.pieces_eligible_full(
    arg_calling_provider_id INTEGER,
    arg_limit INTEGER,
    arg_only_tenant_id SMALLINT, -- use 0 for ~any~
    arg_include_sourceless BOOL,
    arg_only_orglocal BOOL
  ) RETURNS TABLE (
    piece_id BIGINT,
    piece_log2_size SMALLINT,
    has_sources_http BOOL,
    has_sources_fil_active BOOL,
    piece_cid TEXT,
    tenant_ids SMALLINT[]
  )
LANGUAGE sql PARALLEL RESTRICTED STABLE STRICT AS $$

  WITH
    sp AS MATERIALIZED (
      SELECT
          sp.org_id,
          sp.city_id,
          sp.country_id,
          sp.continent_id,
          ( COALESCE( (spi.info->'sector_log2_size')::BIGINT, 0 ) >= 36 ) AS can_seal_64g_sectors
        FROM egd.providers sp
        LEFT JOIN egd.providers_info spi USING ( provider_id )
      WHERE
        sp.provider_id = arg_calling_provider_id
    ),
    enabled_tenants AS MATERIALIZED (
      SELECT tp.tenant_id
        FROM egd.tenants_providers tp
      WHERE
        tp.provider_id = arg_calling_provider_id
          AND
        NOT COALESCE( ( tp.tenant_provider_meta->'inactivated' )::BOOL, false )
          AND
        ( arg_only_tenant_id = 0 OR arg_only_tenant_id = tp.tenant_id )
    )
    ,
    claiming_tenants AS (
      SELECT
          piece_id,
          ARRAY_AGG( et.tenant_id ) AS tenant_ids
        FROM egd.mv_pieces_availability pa, sp, enabled_tenants et

      WHERE

        et.tenant_id = ANY( pa.potential_tenant_ids )

          AND

        NOT EXISTS (
          SELECT 42
            FROM egd.mv_overreplicated_total o_total
          WHERE
            o_total.piece_id = pa.piece_id
              AND
            o_total.tenant_id = et.tenant_id
        )

          AND


        NOT EXISTS (
          SELECT 42
            FROM egd.mv_overreplicated_org o_org
          WHERE
            o_org.piece_id = pa.piece_id
              AND
            o_org.tenant_id = et.tenant_id
              AND
            o_org.org_id = sp.org_id
        )

          AND

        NOT EXISTS (
          SELECT 42
            FROM egd.mv_overreplicated_city o_city
          WHERE
            o_city.piece_id = pa.piece_id
              AND
            o_city.tenant_id = et.tenant_id
              AND
            o_city.city_id = sp.city_id
        )

          AND

        NOT EXISTS (
          SELECT 42
            FROM egd.mv_overreplicated_country o_country
          WHERE
            o_country.piece_id = pa.piece_id
              AND
            o_country.tenant_id = et.tenant_id
              AND
            o_country.country_id = sp.country_id
        )

          AND

        NOT EXISTS (
          SELECT 42
            FROM egd.mv_overreplicated_continent o_continent
          WHERE
            o_continent.piece_id = pa.piece_id
              AND
            o_continent.tenant_id = et.tenant_id
              AND
            o_continent.continent_id = sp.continent_id
        )
      GROUP BY piece_id
    )

  SELECT
      pa.piece_id,
      pa.piece_log2_size,
      pa.http_available AS has_sources_http,
      ( pa.coarse_latest_active_end_epoch IS NOT NULL ) AS has_sources_fil_active,
      pa.piece_cid,
      claiming_tenants.tenant_ids
    FROM egd.mv_pieces_availability pa, sp , claiming_tenants

  WHERE
    --
    -- there is a list of interested+enabled tenants
    claiming_tenants.piece_id = pa.piece_id

      AND

    --
    -- can we seal it ourselves?
    (
      sp.can_seal_64g_sectors
        OR
      NOT pa.requires_64g_sector
    )

      AND

    --
    -- sourcelessness
    (
      arg_include_sourceless
        OR
      pa.has_sources = true
    )

      AND

    --
    -- orglocal only
    (
      NOT arg_only_orglocal
        OR
      EXISTS (
        SELECT 42
          FROM egd.mv_orglocal_presence op
        WHERE
          op.piece_id = pa.piece_id
            AND
          op.org_id = sp.org_id
      )
    )

      AND

    --
    -- exclude my own known/in-flight
    NOT EXISTS (
      SELECT 42
        FROM egd.mv_deals_prefiltered_for_repcount dpfr
      WHERE
        dpfr.piece_id = pa.piece_id
          AND
        dpfr.provider_id = arg_calling_provider_id
    )

      AND

    --
    -- exclude my own pre-flight
    NOT EXISTS (
      SELECT 42
        FROM egd.proposals pr
      WHERE
        pr.piece_id = pa.piece_id
          AND
        pr.provider_id = arg_calling_provider_id
          AND
        pr.proposal_failstamp = 0
          AND
        pr.activated_deal_id IS NULL
    )

  ORDER BY display_sort

  LIMIT arg_limit
$$;

-- END SQLGEN

COMMIT;
