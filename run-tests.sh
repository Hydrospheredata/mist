export PYTHONPATH=$SPARK_HOME/python/:$SPARK_HOME/python/lib/py4j-0.10.1-src.zip:$PYTHONPATH
export PYTHONPATH=$PYTHONPATH:$MIST_HOME/src/main/python
$SPARK_HOME/sbin/start-master.sh
$SPARK_HOME/sbin/start-slave.sh localhost:7077
cd $MIST_HOME
./sbt/sbt -Dconfig.file=configs/docker.conf test
