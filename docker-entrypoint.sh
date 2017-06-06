#!/bin/bash

set -e
cd ${MIST_HOME}

if [ "$1" = 'mist' ]; then
  if [ -e "configs/user.conf" ]; then
    cp -f configs/user.conf configs/default.conf
  fi
  export IP=`ifconfig | sed -En 's/127.0.0.1//;s/.*inet (addr:)?(([0-9]*\.){3}[0-9]*).*/\2/p'`
  echo "$IP    master" >> /etc/hosts

  JAVA_ARGS="-Dmist.akka.cluster.seed-nodes.0=akka.tcp://mist@$IP:2551"
  JAVA_ARGS="$JAVA_ARGS -Dmist.akka.remote.netty.tcp.hostname=$IP"
  exec ./bin/mist-master start --java-args "$JAVA_ARGS" --debug true
elif [ "$1" = 'worker' ]; then 
  if [ ! -z $5 ]; then
    echo $5 | base64 -d  > configs/default.conf
  fi  
  export IP=`getent hosts master | awk '{ print $1 }'`
  export MYIP=`ifconfig | sed -En 's/127.0.0.1//;s/.*inet (addr:)?(([0-9]*\.){3}[0-9]*).*/\2/p'`

  JAVA_ARGS="-Dmist.akka.cluster.seed-nodes.0=akka.tcp://mist@$IP:2551"
  JAVA_ARGS="$JAVA_ARGS -Dmist.akka.remote.netty.tcp.hostname=$MYIP"
  JAVA_ARGS="$JAVA_ARGS -Dmist.akka.remote.netty.tcp.bind-hostname=$MYIP"
  exec ./bin/mist-worker --runner local --name $2 --context $3 --java-args "$JAVA_ARGS" --mode $4 --run-options $6
else
  exec "$@"
fi
