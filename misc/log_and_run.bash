#!/bin/bash

set -eu

LOGDIR="$HOME/LOGS/$(date -u '+%Y-%m-%d')"
mkdir -p "$LOGDIR"

export GOLOG_LOG_FMT=json

"${@:2}" >>"$LOGDIR/$1" 2>&1
