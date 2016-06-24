#!/usr/bin/env bash

export MIST_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ "${SPARK_HOME}" == '' ] || [ ! -d "${SPARK_HOME}" ]
then
    echo "SPARK_HOME is not set"
    exit 1
fi

app=$1

shift
while [[ $# > 1 ]]
do
    key="$1"

  case ${key} in
    --namespace)
      NAMESPACE="$2"
      shift
      ;;
    # TODO: add parameters: config file, app file
  esac

shift
done

if [ "${app}" == 'worker' ]
then
    echo "START WORKER WITH NAMESPACE $NAMESPACE"
    ${SPARK_HOME}/bin/spark-submit --class io.hydrosphere.mist.Worker --driver-java-options "-Dconfig.file=${MIST_HOME}/configs/vagrant.conf" ${MIST_HOME}/target/scala-2.10/mist-assembly-0.2.0.jar ${NAMESPACE}
    exit 0
fi

if [ "${app}" == 'master' ]
then
    echo "START MASTER!"
    ${SPARK_HOME}/bin/spark-submit --class io.hydrosphere.mist.Master --driver-java-options "-Dconfig.file=${MIST_HOME}/configs/vagrant.conf" ${MIST_HOME}/target/scala-2.10/mist-assembly-0.2.0.jar
    exit 0
fi
