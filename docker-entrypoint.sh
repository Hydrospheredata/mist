#!/bin/bash

set -e
cd ${MIST_HOME}

if [ "$1" = 'mist' ]; then
  export IP=`ifconfig | sed -En 's/127.0.0.1//;s/.*inet (addr:)?(([0-9]*\.){3}[0-9]*).*/\2/p'`
  echo "$IP    master" >> /etc/hosts

  export JAVA_OPTS="$JAVA_OPTS -Dmist.cluster.host=$IP"
  shift
  exec ./bin/mist-master start --debug true $@
else
  exec "$@"
fi
