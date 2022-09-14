#!/bin/bash

set -eu
set -o pipefail

export pgconn="service=egd"
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
        UNNEST( ARRAY[ 'f0' || client_id::TEXT, client_address ] )
      FROM egd.clients
    WHERE tenant_id IS NOT NULL
    ORDER BY client_id
  " | "$atcat" "$wwwdir/clients.txt"
}
export -f ex_clients

echo ex_clients \
| xargs -d ' ' -n1 -P4 -I{} bash -c {}
