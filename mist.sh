#!/usr/bin/env bash

export MIST_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

read -d '' help <<- EOF
MIST – is a thin service on top of Spark which makes it possible to execute Scala & Python Spark Jobs from application layers and get synchronous, asynchronous, and reactive results as well as provide an API to external clients.

Usage:
  ./mist.sh master --config <config_file> --jar <mist_assembled_jar>

Report bugs to: https://github.com/hydrospheredata/mist/issues
Up home page: http://hydrosphere.io
EOF


if [ "$1" == "--help" ] || [ "$1" == "-h" ] || [ "$2" == "--help" ] || [ "$2" == "-h" ]
then
    echo "${help}"
    exit 0
fi

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

    --config)
      CONFIG_FILE="$2"
      shift
      ;;

    --jar)
      JAR_FILE="$2"
      shift
      ;;
  esac

shift
done

if [ "${CONFIG_FILE}" == '' ] || [ ${JAR_FILE} == '' ]
then
    echo "${help}"
    exit 1
fi

if [ "${app}" == 'worker' ]
then
    if [ "${NAMESPACE}" == '' ]
    then
        echo "You must specify --namespace to run Mist worker"
        exit 3
    fi
    ${SPARK_HOME}/bin/spark-submit --class io.hydrosphere.mist.Worker --driver-java-options "-Dconfig.file=${CONFIG_FILE}" "$JAR_FILE" ${NAMESPACE}
    exit 0
fi

if [ "${app}" == 'master' ]
then
    ${SPARK_HOME}/bin/spark-submit --class io.hydrosphere.mist.Master --driver-java-options "-Dconfig.file=${CONFIG_FILE}" "$JAR_FILE"
    exit 0
fi