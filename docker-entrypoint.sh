#!/bin/bash
set -e
export PYTHONPATH=$MIST_HOME/src/main/python:$SPARK_HOME/python/:`readlink -f $SPARK_HOME/python/lib/py4j*`:$PYTHONPATH
cd $MIST_HOME

if [ "$1" = 'tests' ]; then
  ./sbt/sbt assembly
  ./sbt/sbt -DsparkVersion=${SPARK_VERSION} "project examples" package
  $SPARK_HOME/sbin/start-master.sh
  $SPARK_HOME/sbin/start-slave.sh localhost:7077
  ./sbt/sbt -DsparkVersion=$SPARK_VERSION -Dconfig.file=configs/docker.conf "project examples" package "project mist" test
  bash
elif [ "$1" = 'mist' ]; then
  ./bin/start master --config configs/docker.conf --jar target/scala-*/mist-assembly-*.jar
elif [ "$1" = 'dev' ]; then
  ./sbt/sbt assembly
  ./sbt/sbt -DsparkVersion=${SPARK_VERSION} "project examples" package
  ./bin/start master --config configs/docker.conf --jar target/scala-*/mist-assembly-*.jar
else
  exec "$@"
fi
