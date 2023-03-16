#!/usr/bin/env bash

cd `dirname $0`
cd ..
MY_APP_HOME=`pwd`

export MY_APP_PID=${MY_APP_HOME}/bin/app.pid
export APP_CLASS_NAME="com.github.testing.jsonfilesender.JsonFileKafkaSender"

function wait_for_server_to_die() {
  local pid
  local count
  pid=$1
  timeout=$2
  count=0
  timeoutTime=$(date "+%s")
  let "timeoutTime+=$timeout"
  currentTime=$(date "+%s")
  forceKill=1

  while [[ $currentTime -lt $timeoutTime ]]; do
    $(kill ${pid} > /dev/null 2> /dev/null)
    if kill -0 ${pid} > /dev/null 2>&1; then
      sleep 3
    else
      forceKill=0
      break
    fi
    currentTime=$(date "+%s")
  done

  if [[ forceKill -ne 0 ]]; then
    $(kill -9 ${pid} > /dev/null 2> /dev/null)
  fi
}

if [[ ! -f "${MY_APP_PID}" ]]; then
    echo "server ${APP_CLASS_NAME} is not running"
else
    pid=$(cat ${MY_APP_PID})
    if [[ -z "${pid}" ]]; then
      echo "server ${APP_CLASS_NAME} is not running"
    else
      wait_for_server_to_die $pid 40
      $(rm -f ${MY_APP_PID})
      echo "Pid: ${pid} for ${APP_CLASS_NAME} was killed "
    fi
fi

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

checkJavaServiceStatus "${APP_CLASS_NAME}"
