#!/bin/bash
set -e

/usr/sbin/sshd
sed -i "s%\${JAVA_HOME}%${JAVA_HOME}%" ${HADOOP_HOME}/etc/hadoop/hadoop-env.sh

if [ "$1" = 'start' ]; then
  $HADOOP_HOME/sbin/start-dfs.sh
  if [ -d ${MIST_HOME} ]; then
    cd ${MIST_HOME} && ${MIST_HOME}/sbt/sbt -DsparkVersion=${SPARK_VERSION} "project examples" package
    ${HADOOP_HOME}/bin/hadoop fs -put ${MIST_HOME}/examples/target/scala-*/mist_examples_*.jar /
    ${HADOOP_HOME}/bin/hadoop fs -put ${MIST_HOME}/src/test/python/simple_spark_context.py /
  fi
  ${HADOOP_HOME}/bin/hadoop fs -ls /
  export lastLog=`ls -t ${HADOOP_HOME}/logs/hadoop-root-namenode-*.log | head -1`
  tail -f $lastLog
else
  exec "$@"
fi
