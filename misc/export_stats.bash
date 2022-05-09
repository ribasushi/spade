#!/bin/bash

set -eu
set -o pipefail

export pgconn="service=evergreen"
export wwwdir="$HOME/WEB/public"
export atcat="$( dirname "${BASH_SOURCE[0]}" )/atomic_cat.bash"

###
### Only execute one version of exporter concurrently
###
[[ -z "${STAT_EXPORTER_LOCKFILE:-}" ]] \
&& export STAT_EXPORTER_LOCKFILE="/dev/shm/stat_exporter.lock" \
&& exec /usr/bin/flock -en "$STAT_EXPORTER_LOCKFILE" "$0" "$@"
###
###
###

touch $wwwdir/.run_start

ex_clients() {
  psql $pgconn -At -c "
    SELECT
        UNNEST( ARRAY[ client_id, client_address ] )
      FROM clients
    WHERE is_affiliated
    ORDER BY client_id
  " | "$atcat" "$wwwdir/clients.txt"
}
export -f ex_clients

ex_qap() {
  psql $pgconn <<<"
    \pset footer off
    SELECT
      provider_id,
      active,
      FORMAT( '%22s', CASE WHEN sp_local = 0 THEN '0 TiB' ELSE
        (
          (
            (10*sp_local)
              +
            (1::BIGINT<<30)
              -
            1
          )
            /
          (1::BIGINT<<30)
            /
          1024::NUMERIC
        )::NUMERIC(10,3) || ' TiB'
      END) AS \"potential_sp_local_QAP\",
      FORMAT( '%13s', CASE WHEN padded_size = 0 THEN '0 TiB' ELSE
        (
          (
            (10*padded_size)
              +
            (1::BIGINT<<30)
              -
            1
          )
            /
          (1::BIGINT<<30)
            /
          1024::NUMERIC
        )::NUMERIC(10,3) || ' TiB'
      END) AS \"Evergreen_QAP\",
      earliest_deal::DATE,
      latest_deal::DATE
    FROM (
      (
        SELECT
            'PROPOSED' AS provider_id,
            NULL::BOOL AS active,
            SUM( p.padded_size ) AS padded_size,
            MIN( pr.entry_created ) AS earliest_deal,
            MAX( pr.entry_created ) AS latest_deal,
            NULL::BIGINT AS sp_local,
            -2 AS rank
          FROM proposals pr
          JOIN pieces p USING ( piece_cid )
        WHERE
          NOT COALESCE( (p.meta->'inactive')::BOOL, false )
            AND
          pr.proposal_success_cid IS NOT NULL
            AND
          pr.proposal_failstamp = 0
            AND
          pr.activated_deal_id IS NULL
      )
        UNION ALL
      (
        SELECT
            'ACTIVE' AS provider_id,
            NULL::BOOL AS active,
            SUM( p.padded_size ) AS padded_size,
            MIN( pd.sector_start_time ) AS earliest_deal,
            MAX( pd.sector_start_time ) AS latest_deal,
            NULL::BIGINT AS sp_local,
            -1 AS rank
          FROM published_deals pd
          JOIN pieces p USING( piece_cid )
          JOIN clients c USING ( client_id )
        WHERE
          NOT COALESCE( (pd.meta->'inactive')::BOOL, false )
            AND
          NOT COALESCE( (p.meta->'inactive')::BOOL, false )
            AND
          pd.status = 'active'
            AND
          c.is_affiliated
      )
        UNION ALL
      (
        SELECT
          *,
          RANK() OVER ( ORDER BY active DESC, padded_size DESC, sp_local DESC NULLS LAST, provider_id ) AS rank
        FROM (
          SELECT
              pr.provider_id,
              pr.is_active AS active,
              SUM( COALESCE( p.padded_size, 0 ) ) AS padded_size,
              MIN( pd.sector_start_time ) AS earliest_deal,
              MAX( pd.sector_start_time ) AS latest_deal,
              -- sum up sp_local sizes
              (
                WITH
                  providers_in_org AS (
                    SELECT provider_id FROM providers WHERE org_id IN ( SELECT city FROM providers WHERE provider_id = pr.provider_id )
                  )
                SELECT SUM( padded_size )
                  FROM deallist_eligible de
                WHERE
                  de.provider_id = pr.provider_id
                    AND
                  de.end_time < expiration_cutoff()
                    AND
                  -- the limit of active nonexpiring + in-fight deals within my org is not violated
                  max_per_org() > (
                    (
                      SELECT COUNT(*)
                        FROM published_deals pd
                        JOIN clients c USING ( client_id )
                        JOIN providers_in_org USING ( provider_id )
                      WHERE
                        pd.piece_cid = de.piece_cid
                          AND
                        c.is_affiliated
                          AND
                        pd.status = 'active'
                          AND
                        NOT COALESCE( (pd.meta->'inactive')::BOOL, false )
                          AND
                        pd.end_time > expiration_cutoff()
                    )
                      +
                    (
                      SELECT COUNT(*)
                        FROM proposals pr
                        JOIN providers_in_org USING ( provider_id )
                      WHERE
                        pr.piece_cid = de.piece_cid
                          AND
                        pr.proposal_failstamp = 0
                          AND
                        pr.activated_deal_id IS NULL
                    )
                  )
              ) AS sp_local
            FROM published_deals pd
            JOIN pieces p
              ON
                p.piece_cid = pd.piece_cid
                  AND
                NOT COALESCE( (p.meta->'inactive')::BOOL, false )
                  AND
                NOT COALESCE( (pd.meta->'inactive')::BOOL, false )
            JOIN clients c
              ON c.client_id = pd.client_id AND c.is_affiliated AND pd.status = 'active'
            -- the right-join is here to get us ALL providers against maybe-just the affiliated-valid-deals
            RIGHT JOIN providers pr USING ( provider_id )
          WHERE
            -- this UNION only contains the registered ( active and/or inactive SPs)
            -- next UNION has the 'anyone else with pending stuffz'
            pr.org_id != ''
          GROUP BY pr.provider_id, pr.is_active
        ) ssq
      )
        UNION ALL
      (
        SELECT
          *,
          1000000 + RANK() OVER ( ORDER BY active DESC, padded_size DESC, sp_local DESC NULLS LAST, provider_id ) AS rank
        FROM (
          SELECT
              de.provider_id,
              NULL::BOOL AS active,
              NULL::BIGINT AS padded_size,
              NULL::TIMESTAMP WITH TIME ZONE AS earliest_deal,
              NULL::TIMESTAMP WITH TIME ZONE AS latest_deal,
              SUM( p.padded_size ) AS sp_local
            FROM deallist_eligible de
            JOIN pieces p USING ( piece_cid )
            JOIN providers pr USING ( provider_id )
          WHERE
            -- only the un-org-ed ones
            pr.org_id = ''
              AND
            de.end_time < expiration_cutoff()
          GROUP BY de.provider_id
        ) ssq
      )
    ) sq
    ORDER BY rank, sp_local DESC NULLS LAST
  " | "$atcat" "$wwwdir/qap.txt"
}
export -f ex_qap

echo ex_qap ex_clients \
| xargs -d ' ' -n1 -P4 -I{} bash -c {}
