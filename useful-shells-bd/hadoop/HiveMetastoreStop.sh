#!/usr/bin/env bash


APP_ENV_BIN=$(cd $(dirname $0) && pwd)
source ${APP_ENV_BIN}/gmall-common.sh

killServiceByName "hive.metastore.HiveMetaStore"
