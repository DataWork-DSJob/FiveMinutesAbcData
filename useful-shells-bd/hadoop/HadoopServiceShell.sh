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

echo "CMD_NAME: ${CMD_NAME} ; Shell: $0"
case $CMD_NAME in
  "start")
    ${HADOOP_HOME}/sbin/start-dfs.sh > /dev/null
    ${HADOOP_HOME}/sbin/start-yarn.sh > /dev/null
    ${HADOOP_HOME}/sbin/mr-jobhistory-daemon.sh start historyserver > /dev/null
    ${HADOOP_HOME}/sbin/yarn-daemon.sh start timelineserver > /dev/null
    sleep 2
;;
  "stop")
    ${HADOOP_HOME}/sbin/stop-all.sh > /dev/null 2>&1
    ${HADOOP_HOME}/sbin/mr-jobhistory-daemon.sh stop historyserver > /dev/null 2>&1
    ${HADOOP_HOME}/sbin/yarn-daemon.sh stop timelineserver > /dev/null 2>&1
    sleep 2
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
checkJavaServiceStatus "server.namenode.NameNode"
checkJavaServiceStatus "server.namenode.SecondaryNameNode"
checkJavaServiceStatus "server.datanode.DataNode"
checkJavaServiceStatus "server.nodemanager.NodeManager"
checkJavaServiceStatus "server.resourcemanager.ResourceManager"
checkJavaServiceStatus "hs.JobHistoryServer"
checkJavaServiceStatus "server.applicationhistoryservice.ApplicationHistoryServer"

echo "------------ ***  End CheckService for shell : $0  *** ------------"
