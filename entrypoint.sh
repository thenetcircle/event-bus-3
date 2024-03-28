#!/usr/bin/env sh

CMD=$1

if [ $# -gt 0 ]; then
  shift
fi

case $CMD in
  consumer)
    exec python3 eventbus/app_consumer.py "$@"
    ;;
  producer)
    exec /usr/local/bin/gunicorn eventbus.app_producer:app -k uvicorn.workers.UvicornWorker -b 0.0.0.0:8000 -w 1 "$@"
    ;;
  test)
    exec pytest "$@"
    ;;
  coverage)
    coverage run -m pytest
    coverage report -m
    echo "all done!"
    ;;
  *)
    echo "Usage: {consumer|producer|test} ..."
    exit 1
    ;;
esac
