#!/usr/bin/env bash

if [ ! -x "$(command -v isort 2>/dev/null)" ]; then
  echo 'can not find "isort" command in current env.'
  exit 1
fi

if [ ! -x "$(command -v black 2>/dev/null)" ]; then
  echo 'can not find "black" command in current env.'
  exit 1
fi

TARGET=("eventbus tests setup.py")
if [ "$1" != "" ]; then
  TARGET=("$@")
fi

ROOT_DIR=$(dirname $(dirname "$(realpath "$0")"))
cd $ROOT_DIR || exit 1

isort ${TARGET[@]} && \
black --target-version py38 ${TARGET[@]}