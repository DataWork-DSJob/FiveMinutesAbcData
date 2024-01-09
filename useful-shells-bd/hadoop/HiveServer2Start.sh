#!/usr/bin/env bash

nohup hiveserver2 > ${HIVE_HOME}/log/hiveserver2.log 2>&1 & 


function checkServiceStatus(){
  SERVICE_NAME_KEY=$1;
  SERVICE_PID=$(ps ax|grep java|grep -v grep|grep ${SERVICE_NAME_KEY} |awk '{print $1}' )
  if [ -z "$SERVICE_PID" ];then
    sleep 2
    SERVICE_PID=$(ps ax|grep java|grep -v grep|grep ${SERVICE_NAME_KEY} |awk '{print $1}' )
    echo -e "Service:\033[31m ${SERVICE_NAME_KEY} is Stopped and not find PID ${SERVICE_PID} \033[0m"
  else
    echo "Service:${SERVICE_NAME_KEY} is OK. Running at pid: ${SERVICE_PID}"
  fi
}

sleep 4
checkServiceStatus "server.HiveServer2"


