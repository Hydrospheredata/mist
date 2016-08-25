export PYTHONPATH=$MIST_HOME/src/main/python:$SPARK_HOME/python/:`readlink -f $SPARK_HOME/python/lib/py4j*`:$PYTHONPATH
$SPARK_HOME/sbin/start-master.sh
$SPARK_HOME/sbin/start-slave.sh localhost:7077
cd $MIST_HOME
./sbt/sbt -Dconfig.file=configs/docker.conf test
