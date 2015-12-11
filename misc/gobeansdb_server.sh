#!/bin/bash
# based on http://serverfault.com/a/360230/105615
#

# 注意这里的端口号需要和 gobeansproxy/conf/route.yaml 的端口号一致
ports="7980 7981 7982 7983"
cmd="`which gobeansdb`"

basedir="/tmp/gobeansdb_$USER"
prog="gobeansdb"

function gen_conf()
{
    ./misc/gen_config.py -d $basedir
}

function start()
{
    port="$1"
    if [ `ps -ef | grep "$cmd" | grep -c $port` -ge 1 ]; then
        echo "Beansdb server already started on port '$port'"
    else
        gen_conf
        $cmd -confdir $basedir/$port/conf &
        echo "Starting the beansdb server on port '$port'... "
    fi
}

function stop()
{
    port="$1"
    if [ `ps -ef | grep "$cmd" | grep -c $port` -eq 0 ]; then
        echo $"Stopped the beansdb server on port '$port'... "
    else
        kill -TERM `ps -ef | grep "$cmd" | grep $port | grep -v grep | awk '{ print $2 }'`
        echo "Stopping the beansdb server on port '$port'... "
    fi
    rm -rf $basedir
}

case "$1" in
    start)
        if [ -n "$2" ]; then
            start $2
        else
            for port in $ports; do
                start $port
            done
        fi
        ;;
    stop)
        if [ -n "$2" ]; then
            port="$2"
            stop $port
        else
            for port in $ports; do
                stop $port
            done
        fi
        ;;
    restart)
        if [ -n "$2" ]; then
            stop $2
            sleep 1
            start $2
        else
            for port in $ports; do
                stop $port
                sleep 1
                start $port
            done
        fi
        ;;
    *)
        printf 'Usage: %s {start|stop|restart} [port]\n' "$prog"
        exit 1
        ;;
esac