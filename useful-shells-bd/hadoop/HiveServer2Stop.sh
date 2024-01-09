#!/usr/bin/env bash

function kill_action(){
    TO_KILL_SERVICE_KEY=$1
    KILL_PID=$(ps ax|grep java|grep -v grep|grep ${TO_KILL_SERVICE_KEY} |awk '{print $1}' )
    echo "To kill service:${TO_KILL_SERVICE_KEY} . 即将kill的pid=${KILL_PID}"
    if [ -z "$KILL_PID" ];then
      echo -e "Service:\033[31m No server:${TO_KILL_SERVICE_KEY} to stop \033[0m"
      exit 1
    else
      kill ${KILL_PID}
      sleep 3
    fi
    while true
    do
      KILL_PID=$(ps ax|grep java|grep -v grep|grep ${TO_KILL_SERVICE_KEY} |awk '{print $1}' )
      if [ -z "$KILL_PID" ]; then
        break;
      else

        echo "$TO_KILL_SERVICE_KEY server pid still here . 再次kill -9 $KILL_PID"
        kill -9 ${KILL_PID}
        sleep 2
      fi
    done
    KILL_PID=$(ps ax|grep java|grep -v grep|grep ${TO_KILL_SERVICE_KEY} |awk '{print $1}' )
    echo "${TO_KILL_SERVICE_KEY} stopped  服务已停止 $KILL_PID"
}

kill_action "server.HiveServer2"

