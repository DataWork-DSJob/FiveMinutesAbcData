#!/bin/bash


CMD_NAME=$1
if [ "x$1" == "x" ]; then
  CMD_NAME="status"
fi

APP_ENV_BIN=$(cd $(dirname $0) && pwd)
source ${APP_ENV_BIN}/gmall-common.sh


case $CMD_NAME in
  "start")
    sh start-hbase.sh
;;
  "stop")
    sh stop-hbase.sh
;;
  "status")
    echo "OnlyQuery ServerStatus: HMaster , HRegionServer"
;;
  *)
  {
    echo "Expect: start/stop/status, but actual: $1 . Only Check Server Status"
  }
esac

checkServiceStatus "hbase.master.HMaster"
checkServiceStatus "hbase.regionserver.HRegionServer"

