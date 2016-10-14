#!/bin/bash
set -e
export PYTHONPATH=${MIST_HOME}/src/main/python:${SPARK_HOME}/python/:`readlink -f ${SPARK_HOME}/python/lib/py4j*`:${PYTHONPATH}
cd ${MIST_HOME}

if [ "$1" = 'tests' ]; then
  ./sbt/sbt -DsparkVersion=${SPARK_VERSION} assembly
  ./sbt/sbt -DsparkVersion=${SPARK_VERSION} -Dconfig.file=src/test/resources/tests-${SPARK_VERSION}.conf "project examples" package "project mist" test
  bash
elif [ "$1" = 'mist' ]; then
  export IP=`ifconfig | sed -En 's/127.0.0.1//;s/.*inet (addr:)?(([0-9]*\.){3}[0-9]*).*/\2/p'`
  echo "$IP    master" >> /etc/hosts
  ./bin/mist start master --config configs/docker.conf --jar target/scala-*/mist-assembly-*.jar
elif [ "$1" = 'worker' ]; then 
  ./bin/mist start worker --runner local --namespace $2 --config configs/docker.conf --jar target/scala-*/mist-assembly-*.jar
elif [ "$1" = 'dev' ]; then
  ./sbt/sbt -DsparkVersion=${SPARK_VERSION} assembly
  ./sbt/sbt -DsparkVersion=${SPARK_VERSION} "project examples" package
  ./bin/mist start master --config configs/docker.conf --jar target/scala-*/mist-assembly-*.jar
else
  exec "$@"
fi
