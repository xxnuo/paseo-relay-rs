#!/bin/sh
set -eu
ulimit -n 65536

host=${PASEO_RELAY_HOST-0.0.0.0}
port=${PASEO_RELAY_PORT-4000}
case "$host" in ''|*[!0-9a-fA-F:.]*) echo 'Invalid PASEO_RELAY_HOST' >&2; exit 1;; esac
case "$port" in ''|*[!0-9]*) echo 'Invalid PASEO_RELAY_PORT' >&2; exit 1;; esac
if ! [ "$port" -ge 1 ] 2>/dev/null || ! [ "$port" -le 65535 ] 2>/dev/null; then
    echo 'Invalid PASEO_RELAY_PORT' >&2
    exit 1
fi
case "$host" in *:*) NGINX_LISTEN="[$host]:$port";; *) NGINX_LISTEN="$host:$port";; esac
NGINX_BACKEND_PORT=4001
if [ "$port" -eq 4001 ]; then NGINX_BACKEND_PORT=4002; fi
NGINX_HTTP_IDLE_TIMEOUT=${PASEO_RELAY_HTTP_IDLE_TIMEOUT_MS-15000}
case "$NGINX_HTTP_IDLE_TIMEOUT" in ''|*[!0-9]*) echo 'Invalid PASEO_RELAY_HTTP_IDLE_TIMEOUT_MS' >&2; exit 1;; esac
export NGINX_LISTEN NGINX_BACKEND_PORT NGINX_HTTP_IDLE_TIMEOUT
envsubst '${NGINX_LISTEN} ${NGINX_BACKEND_PORT} ${NGINX_HTTP_IDLE_TIMEOUT}' \
    < /etc/nginx/paseo.conf.template > /tmp/nginx.conf
nginx -t -c /tmp/nginx.conf

relay_pid=''
nginx_pid=''
stop() {
    trap '' TERM INT
    # Keep the proxy alive while Rust drains and sends its retryable closes.
    if [ -n "$relay_pid" ]; then
        kill -TERM "$relay_pid" 2>/dev/null || true
        wait "$relay_pid" 2>/dev/null || true
    fi
    if [ -n "$nginx_pid" ]; then
        kill -QUIT "$nginx_pid" 2>/dev/null || true
        wait "$nginx_pid" 2>/dev/null || true
    fi
}
trap 'stop; exit 0' TERM INT
PASEO_RELAY_HOST=127.0.0.1 PASEO_RELAY_PORT="$NGINX_BACKEND_PORT" /paseo-relay &
relay_pid=$!
for attempt in $(seq 1 100); do
    if wget -q -O /dev/null "http://127.0.0.1:$NGINX_BACKEND_PORT/health" 2>/dev/null; then break; fi
    if ! kill -0 "$relay_pid" 2>/dev/null || [ "$attempt" -eq 100 ]; then
        stop
        exit 1
    fi
    sleep 0.05
done
nginx -c /tmp/nginx.conf -g 'daemon off;' &
nginx_pid=$!
# BusyBox ash's wait -n does not reliably notice the older child exiting.
while kill -0 "$relay_pid" 2>/dev/null && kill -0 "$nginx_pid" 2>/dev/null; do
    sleep 0.1
done
stop
exit 1
