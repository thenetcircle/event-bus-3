#!/usr/bin/env bash

if [ ! -x "$(command -v curl 2>/dev/null)" ]; then
  echo 'can not find "curl" command in current env.'
  exit 1
fi


if [ "$1" = "STAGING" -o "$1" = "staging" ]; then

  curl -X POST \
       -F token=d473611305ed55634979a7406d5d08 \
       -F "ref=main" \
       -F "variables[STAGING]=true" \
       http://gitlab.thenetcircle.lab/api/v4/projects/802/trigger/pipeline

else

  curl -X POST \
       -F token=d473611305ed55634979a7406d5d08 \
       -F "ref=main" \
       http://gitlab.thenetcircle.lab/api/v4/projects/802/trigger/pipeline

fi