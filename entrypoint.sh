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
    flask run --host 0.0.0.0 --port $TRIALSTREAMER_TS_PORT --eager-loading
    ;;
api)
    echo "[entrypoint.sh] Starting Trialstreamer API"
    gunicorn --worker-class gevent --workers $GUNICORN_WORKERS --timeout $GUNICORN_WORKER_TIMEOUT -b 0.0.0.0:$TRIALSTREAMER_TS_PORT server:app
    ;;
cron)
    echo "[entrypoint.sh] Starting Trialstreamer Updates Crontab"
    export > /var/lib/deploy/cron.env
    chmod 0644 /etc/cron.d/crontab
    crontab /etc/cron.d/crontab
    cron -f
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
