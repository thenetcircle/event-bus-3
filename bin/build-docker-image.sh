#!/usr/bin/env bash

ROOT_DIR=$(realpath "$(dirname "$(dirname "$0")")")
cd $ROOT_DIR || exit 1

if [ ! -x "$(command -v docker 2>/dev/null)" ]; then
  echo 'can not find "docker" command in current env.'
  exit 1
fi

usage() {
  echo ""
  echo "USAGE: ${0} [-t TAG] TARGET"
  echo ""
  echo "TARGET includes: consumer, producer, ci"
  echo ""
}

TAG=""
TARGET=""

while [ "$1" != "" ]; do
  PARAM=$(echo $1 | awk -F= '{print $1}')
  case $PARAM in
  -t)
    shift
    TAG="$1"
    ;;
  *)
    if [ "$TARGET" = "" ]; then
      TARGET="$1"
    else
      echo "ERROR: unknown parameter \"$1\""
    fi
    ;;
  esac
  shift
done

if [ "$TARGET" = "consumer" ]; then
  if [ "$TAG" = "" ]; then TAG="event-bus-3-consumer"; fi
  docker build -f dockerfiles/Dockerfile.consumer -t $TAG .
elif [ "$TARGET" = "producer" ]; then
  if [ "$TAG" = "" ]; then TAG="event-bus-3-producer"; fi
  docker build -f dockerfiles/Dockerfile.producer -t $TAG .
elif [ "$TARGET" = "ci" ]; then
  if [ "$TAG" = "" ]; then TAG="event-bus-3-ci"; fi
  docker build -f dockerfiles/Dockerfile.ci -t $TAG .
else
  usage
  exit 1
fi