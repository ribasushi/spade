#!/bin/bash

set -eu
set -o pipefail

tmpname=$( perl -pe 's{ (^|/) ([^/\n]*) $ }{$1.$2.XXXXXX}x' <<<"$1" )
tmpfile=$( mktemp "$tmpname" )

chmod 644 "$tmpfile"
trap 'ec=$?; rm -f -- "$tmpfile"; exit $ec' PIPE INT TERM HUP EXIT

cat > "$tmpfile"

[[ -s "$tmpfile" ]] && mv "$tmpfile" "$1"
