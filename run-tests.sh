export PYTHONPATH=$MIST_HOME/src/main/python:$SPARK_HOME/python/:`readlink -f $SPARK_HOME/python/lib/py4j*`:$PYTHONPATH
$SPARK_HOME/sbin/start-master.sh
$SPARK_HOME/sbin/start-slave.sh localhost:7077
cd $MIST_HOME

app=$1

shift
while [[ $# > 1 ]]
do
    key="$1"

  case ${key} in
    --sparkVersion)
      SPARK_VERSION="$2"
      shift
      ;;
  esac

shift
done

if [ "${SPARK_VERSION}" == '' ] || [ ! -d "${SPARK_VERSION}" ]
then
    #echo "SPARK_VERSION is not set"
    #exit 1
    SPARK_VERSION = 1.5.2
fi
./sbt/sbt -DsparkVersion=${SPARK_VERSION} -Dconfig.file=configs/docker.conf test
$SPARK_HOME/sbin/stop-master.sh
$SPARK_HOME/sbin/stop-slave.sh
