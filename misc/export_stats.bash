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
    SELECT provider_id, active, PG_SIZE_PRETTY(qap) AS \"Evergreen_QAP\", earliest_deal::DATE, latest_deal::DATE FROM (
      (
        SELECT
            'PROPOSED' AS provider_id,
            NULL::BOOL AS active,
            10 * SUM( p.padded_size ) AS qap,
            MIN( pr.entry_created ) AS earliest_deal,
            MAX( pr.entry_created ) AS latest_deal,
            -2 AS rank
          FROM proposals pr
          JOIN pieces p USING ( piece_cid )
        WHERE
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
            10 * SUM( p.padded_size ) AS qap,
            MIN( pd.sector_start_time ) AS earliest_deal,
            MAX( pd.sector_start_time ) AS latest_deal,
            -1 AS rank
          FROM published_deals pd
          JOIN pieces p USING( piece_cid )
          JOIN clients c USING ( client_id )
        WHERE
          pd.status = 'active'
            AND
          c.is_affiliated
      )
        UNION ALL
      (
        SELECT
          *,
          RANK() OVER ( ORDER BY active DESC, qap DESC, provider_id ) AS rank
        FROM (
          SELECT
              pr.provider_id,
              pr.is_active AS active,
              10 * SUM( COALESCE( p.padded_size, 0 ) ) AS qap,
              MIN( pd.sector_start_time ) AS earliest_deal,
              MAX( pd.sector_start_time ) AS latest_deal
            FROM published_deals pd
            JOIN clients c
              ON c.client_id = pd.client_id AND c.is_affiliated AND pd.status = 'active'
            JOIN pieces p USING( piece_cid )
            RIGHT JOIN providers pr USING ( provider_id )
          WHERE
            pr.org_id != ''
          GROUP BY pr.provider_id, pr.is_active
        ) ssq
      )
    ) sq
    ORDER BY rank
  " | "$atcat" "$wwwdir/qap.txt"
}
export -f ex_qap

echo ex_qap ex_clients \
| xargs -d ' ' -n1 -P4 -I{} bash -c {}
