#export SPARK_HOME="/home/vagrant/spark-1.5.2-bin-hadoop2.6/"
#export PYTHONPATH=$SPARK_HOME/python/:$SPARK_HOME/python/lib/py4j-0.8.2.1-src.zip:$PYTHONPATH

import py4j.java_gateway
import pyspark
import sys, getopt, traceback, json, re

from py4j.java_gateway import java_import, JavaGateway, GatewayClient
from py4j.java_collections import SetConverter, MapConverter, ListConverter
from py4j.protocol import Py4JJavaError

from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.types import *
from pyspark.rdd import RDD
from pyspark.files import SparkFiles
from pyspark.storagelevel import StorageLevel
from pyspark.accumulators import Accumulator, AccumulatorParam
from pyspark.broadcast import Broadcast
from pyspark.serializers import MarshalSerializer, PickleSerializer

# for back compatibility
from pyspark.sql import SQLContext, HiveContext, SchemaRDD, Row

###################################################################
_client = GatewayClient(port=int(sys.argv[1]))
_gateway = JavaGateway(_client, auto_convert = True)
_entry_point = _gateway.entry_point
java_import(_gateway.jvm, "org.apache.spark.SparkContext")
java_import(_gateway.jvm, "org.apache.spark.SparkEnv")
java_import(_gateway.jvm, "org.apache.spark.SparkConf")
java_import(_gateway.jvm, "org.apache.spark.api.java.*")
java_import(_gateway.jvm, "org.apache.spark.api.python.*")
java_import(_gateway.jvm, "org.apache.spark.mllib.api.python.*")
java_import(_gateway.jvm, "org.apache.spark.*")

def _getSparkContext():
  try:
    ScalaSparkContextWrapper = _entry_point.ScalaSparkContextWrapper()
    sconf = ScalaSparkContextWrapper.getSparkConf(sys.argv[2])
    conf = SparkConf(_jvm = _gateway.jvm, _jconf = sconf)
    jsc = ScalaSparkContextWrapper.getSparkContext(sys.argv[2])
    sc = SparkContext(jsc=jsc, gateway=_gateway, conf=conf)
    return sc

  except Py4JJavaError:
    print("except Py4JJavaError")
    print(traceback.format_exc())
    ScalaSparkContextWrapper.setStatementsFinished(traceback.format_exc(), True)
    return None

  except Exception:
    print(traceback.format_exc())
    ScalaSparkContextWrapper.setStatementsFinished(traceback.format_exc(), True)
    return None

_sc = _getSparkContext()

def getSparkContext():
  return _sc

def _getSqlContext():
  try:
    ScalaSparkContextWrapper = _entry_point.ScalaSparkContextWrapper()
    sqlc = SQLContext(_sc, ScalaSparkContextWrapper.getSqlContext(sys.argv[2]))
    return  sqlc

  except Py4JJavaError:
    print("except Py4JJavaError")
    print(traceback.format_exc())
    ScalaSparkContextWrapper.setStatementsFinished(traceback.format_exc(), True)
    return None

  except Exception:
    print(traceback.format_exc())
    ScalaSparkContextWrapper.setStatementsFinished(traceback.format_exc(), True)
    return None
_sqlc = _getSqlContext()

def getSqlContext():
  return _sqlc

def _getHiveContext():
  try:
    ScalaSparkContextWrapper = _entry_point.ScalaSparkContextWrapper()
    hc = HiveContext(_sc, ScalaSparkContextWrapper.getHiveContext(sys.argv[2]))
    return  hc

  except Py4JJavaError:
    print("except Py4JJavaError")
    print(traceback.format_exc())
    ScalaSparkContextWrapper.setStatementsFinished(traceback.format_exc(), True)
    return None

  except Exception:
    print(traceback.format_exc())
    ScalaSparkContextWrapper.setStatementsFinished(traceback.format_exc(), True)
    return None

_hc = _getHiveContext()

def getHiveContext():
  return _hc

def getNumbers():
  try:
    java_import(_gateway.jvm, 'java.util.*')
    SimpleDataWrapper = _entry_point.SimpleDataWrapper()

    num = SimpleDataWrapper.get(sys.argv[2])
    l = list()
    count = 0
    size = num.size()
    while count < size:
      l.append(num.head())
      count = count + 1
      num = num.tail()
    return l

  except Py4JJavaError:
    print("except Py4JJavaError")
    print(traceback.format_exc())
    SimpleDataWrapper.setStatementsFinished(traceback.format_exc(), True)
    return None

  except Exception:
    print(traceback.format_exc())
    SimpleDataWrapper.setStatementsFinished(traceback.format_exc(), True)
    return None

def sendResult(result):
  try:
    java_import(_gateway.jvm, 'java.util.*')
    SimpleDataWrapper = _entry_point.SimpleDataWrapper()
    SimpleDataWrapper.set(sys.argv[2], result)

  except Py4JJavaError:
    print("except Py4JJavaError")
    print(traceback.format_exc())
    SimpleDataWrapper.setStatementsFinished(traceback.format_exc(), True)

  except Exception:
    print(traceback.format_exc())
    SimpleDataWrapper.setStatementsFinished(traceback.format_exc(), True)

