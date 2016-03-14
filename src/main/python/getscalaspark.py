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
    sparkContextWrapper = _entry_point.sparkContextWrapper()
    sconf = sparkContextWrapper.getSparkConf(sys.argv[2])
    conf = SparkConf(_jvm = _gateway.jvm, _jconf = sconf)
    jsc = sparkContextWrapper.getSparkContext(sys.argv[2])
    sc = SparkContext(jsc=jsc, gateway=_gateway, conf=conf)
    return sc

  except Py4JJavaError:
    print("except Py4JJavaError")
    print(traceback.format_exc())
    err = _entry_point.errorWrapper()
    err.set(sys.argv[2], traceback.format_exc())

  except Exception:
    print(traceback.format_exc())
    err = _entry_point.errorWrapper()
    err.set(sys.argv[2], traceback.format_exc())

_sc = _getSparkContext()

def getSparkContext():
  return _sc

def _getSqlContext():
  try:
    sparkContextWrapper = _entry_point.sparkContextWrapper()
    sqlc = SQLContext(_sc, sparkContextWrapper.getSqlContext(sys.argv[2]))
    return  sqlc

  except Py4JJavaError:
    print("except Py4JJavaError")
    print(traceback.format_exc())
    err = _entry_point.errorWrapper()
    err.set(sys.argv[2], traceback.format_exc())

  except Exception:
    print(traceback.format_exc())
    err = _entry_point.errorWrapper()
    err.set(sys.argv[2], traceback.format_exc())

def getSqlContext():
  if not hasattr(getSqlContext, "_sqlc"):
    getSqlContext._sqlc = _getSqlContext()
  return getSqlContext._sqlc


def _getHiveContext():
  try:
    sparkContextWrapper = _entry_point.sparkContextWrapper()
    hc = HiveContext(_sc, sparkContextWrapper.getHiveContext(sys.argv[2]))
    return  hc

  except Py4JJavaError:
    print("except Py4JJavaError")
    print(traceback.format_exc())
    err = _entry_point.errorWrapper()
    err.set(sys.argv[2], traceback.format_exc())

  except Exception:
    print(traceback.format_exc())
    err = _entry_point.errorWrapper()
    err.set(sys.argv[2], traceback.format_exc())

def getHiveContext():
  if not hasattr(getHiveContext, "_hc"):
    getHiveContext._hc = _getHiveContext()
  return getHiveContext._hc

def getParameters():
  try:
    java_import(_gateway.jvm, 'java.util.*')
    dataWrapper = _entry_point.dataWrapper()

    parameters = dataWrapper.get(sys.argv[2])
    return parameters

  except Py4JJavaError:
    print("except Py4JJavaError")
    print(traceback.format_exc())
    err = _entry_point.errorWrapper()
    err.set(sys.argv[2], traceback.format_exc())

  except Exception:
    print(traceback.format_exc())
    err = _entry_point.errorWrapper()
    err.set(sys.argv[2], traceback.format_exc())

def sendResult(result):
  try:
    java_import(_gateway.jvm, 'java.util.*')
    dataWrapper = _entry_point.dataWrapper()
    dataWrapper.set(sys.argv[2], result)

  except Py4JJavaError:
    print("except Py4JJavaError")
    print(traceback.format_exc())
    err = _entry_point.errorWrapper()
    err.set(sys.argv[2], traceback.format_exc())

  except Exception:
    print(traceback.format_exc())
    err = _entry_point.errorWrapper()
    err.set(sys.argv[2], traceback.format_exc())