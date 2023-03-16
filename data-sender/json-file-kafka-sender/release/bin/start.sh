#!/bin/bash

APP_CLASS_NAME="com.github.testing.jsonfilesender.JsonFileKafkaSender"


cd `dirname $0`
cd ..
MY_APP_HOME=`pwd`

export MY_APP_PID=$MY_APP_HOME/bin/app.pid
if [[ -f "${MY_APP_PID}" ]]; then
    pid=$(cat ${MY_APP_PID})
    if kill -0 ${pid} >/dev/null 2>&1; then
      echo "${APP_CLASS_NAME} is already running."
      exit 1
    fi
fi

# Define Classpath, 定义jar依赖路径
CLASSPATH="${CLASSPATH}"
if [ -d "${MY_APP_HOME}/config" ]; then
  CLASSPATH=${CLASSPATH}:${MY_APP_HOME}/config;
fi

APP_INSTALL_LIB=${MY_APP_HOME}/lib
if [ ! -d "${APP_INSTALL_LIB}" ]; then
  echo "Missing App Lib and Jars"
    exit 1;
fi
for jarfile in ${APP_INSTALL_LIB}/*.jar; do
  CLASSPATH=${CLASSPATH}:$jarfile;
done

## 如果APP_HOME目录下有jar, 一般是主应用程序, 加到CP中
for jarfile in ${APP_HOME}/*.jar; do
  CLASSPATH=$jarfile:${CLASSPATH};
done
echo "CLASSPATH=${CLASSPATH}:"

# Set Log Dir
MY_APP_LOG_DIR=${MY_APP_HOME}/log
mkdir -p ${MY_APP_LOG_DIR}

MY_JVM_OPTS=$MY_JVM_OPTS

KVM_JAVA_OPTS="$KVM_JAVA_OPTS"
if [ "x$KVM_JAVA_OPTS" == "x" ]; then
  KVM_JAVA_OPTS="-Xmx128m -Xms128m"
fi

# Set Debug options if enabled
# 想对Jax-web调试时, 只需重启前在命令行输入一下: export JAX_WEB_DEBUG_PORT=45001  , 即可本地IDEA通过Remote Attach远程JVM进行调试
if [ "x$KVM_DEBUG_PORT" != "x" ]; then
  # Use the defaults if JAVA_DEBUG_OPTS was not set
  DEFAULT_JAVA_DEBUG_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=$KVM_DEBUG_PORT"
  if [ -z "$JAVA_DEBUG_OPTS" ]; then
    JAVA_DEBUG_OPTS="$DEFAULT_JAVA_DEBUG_OPTS"
  fi
	echo "Enabling Java debug options: $JAVA_DEBUG_OPTS"
  KVM_JAVA_OPTS="${KVM_JAVA_OPTS} ${JAVA_DEBUG_OPTS}"
fi


nohup java ${KVM_JAVA_OPTS} -cp ${CLASSPATH} ${APP_CLASS_NAME} "$@" > ${MY_APP_LOG_DIR}/app.log 2>&1 &

pid=$!
sleep 3
SERVICE_PID=$(jps|grep ${pid} |awk '{print $1}' )
if [[ -z "${SERVICE_PID}" ]]; then
    echo "Server start failed!"
    exit 1
else
    echo "Server start succeed!"
    echo ${pid} > $MY_APP_PID
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

