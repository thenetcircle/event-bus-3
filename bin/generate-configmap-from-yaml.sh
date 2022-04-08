#!/usr/bin/env bash

ROOT_DIR=$(realpath "$(dirname "$(dirname "$0")")")
cd $ROOT_DIR || exit 1

usage() {
  echo ""
  echo "USAGE: ${0} yaml_file > target_location"
  echo ""
}

if [ "$1" = "" ]; then
  usage
  exit 1
fi

YAML_FILE="$1"
CURRENT_TIME="`date +%s`"
YAML_DATA="`cat $YAML_FILE | sed "s/^last_update_time: .*/last_update_time: ${CURRENT_TIME}/" | sed 's/^/    /'`"

cat << EOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: event-bus-3-config
data:
  config: |
$YAML_DATA
EOF