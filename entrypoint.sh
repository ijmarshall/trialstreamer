#!/bin/sh

OPTS=$CMD

if [ -z "$OPTS" ]
then
    OPTS=$1
fi

case "$OPTS" in

api-dev)
    echo "[entrypoint.sh] Starting Trialstreamer API in Development mode"
    export LC_ALL=C.UTF-8
    export LANG=C.UTF-8
    export FLASK_APP=server:flask_app
    export FLASK_ENV=development
    cd /var/lib/deploy/ && flask run --host 0.0.0.0 --port 5000 --eager-loading
    ;;
api)
    echo "[entrypoint.sh] Starting Trialstreamer API"
    cd /var/lib/deploy/ && gunicorn --worker-class gevent --workers $GUNICORN_WORKERS --timeout $GUNICORN_WORKER_TIMEOUT -b 0.0.0.0:5000 server:app
    ;;
*)
    if [ ! -z "$(which $1)" ]
    then
        $@
    else
        echo "Invalid command"
        exit 1
    fi
    ;;
esac
