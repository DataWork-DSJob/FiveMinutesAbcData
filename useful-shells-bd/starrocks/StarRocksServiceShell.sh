#!/bin/bash

CMD_NAME=$1
if [ "x$1" == "x" ]; then
  CMD_NAME="status"
fi

APP_ENV_BIN=$(cd $(dirname $0) && pwd)

function checkJavaServiceStatus(){
  SERVICE_NAME_KEY=$1;
  SERVICE_PID=$(jps -ml|grep ${SERVICE_NAME_KEY} |awk '{print $1}' )
  if [ -z "$SERVICE_PID" ];then
    sleep 2
    SERVICE_PID=$(ps ax|grep java|grep -v grep|grep ${SERVICE_NAME_KEY} |awk '{print $1}' )
    echo -e "Service:\033[31m ${SERVICE_NAME_KEY} is Stopped and not find PID ${SERVICE_PID} \033[0m"
  else
    echo "Service:${SERVICE_NAME_KEY} is OK. Running at pid: ${SERVICE_PID}"
  fi
}

function checkPsLinuxProcessStatus(){
  sleep 1
  SERVICE_NAME_KEY=$1;
  SERVICE_PID=$(ps ax|grep -v grep|grep ${SERVICE_NAME_KEY} |awk '{print $1}' )
  if [ -z "$SERVICE_PID" ];then
    echo -e "Service:\033[31m ${SERVICE_NAME_KEY} is Stopped and not find PID ${SERVICE_PID} \033[0m"
  else
    echo "Service:${SERVICE_NAME_KEY} is OK. Running at pid: ${SERVICE_PID}"
  fi
}


echo "CMD_NAME: ${CMD_NAME} ; Shell: $0"
case $CMD_NAME in
  "start")
    ${STARROCKS_HOME}/fe/bin/start_fe.sh --daemon
    sleep 2
    ${STARROCKS_HOME}/be/bin/start_be.sh --daemon
    sleep 3
;;
  "stop")
    ${STARROCKS_HOME}/fe/bin/stop_fe.sh
    sleep 2
    ${STARROCKS_HOME}/be/bin/stop_be.sh
    sleep 3
;;
  "status")
    sleep 1
;;
  *)
  {
    echo "Expect: start/stop/status, but actual: $1 . Only Check Server Status"
  }
esac


echo "------------ ***  Start CheckServiceStatus for : $0  *** ------------"
sleep 1
checkJavaServiceStatus "com.starrocks.StarRocksFE"
checkPsLinuxProcessStatus "starrocks_be"
echo "------------ ***  End CheckService for shell : $0  *** ------------"
