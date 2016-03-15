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
class Job:
  def __init__(self):

    self._client = GatewayClient(port=int(sys.argv[1]))
    self._gateway = JavaGateway(self._client, auto_convert = True)
    self._entry_point = self._gateway.entry_point

    java_import(self._gateway.jvm, "org.apache.spark.SparkContext")
    java_import(self._gateway.jvm, "org.apache.spark.SparkEnv")
    java_import(self._gateway.jvm, "org.apache.spark.SparkConf")
    java_import(self._gateway.jvm, "org.apache.spark.api.java.*")
    java_import(self._gateway.jvm, "org.apache.spark.api.python.*")
    java_import(self._gateway.jvm, "org.apache.spark.mllib.api.python.*")
    java_import(self._gateway.jvm, "org.apache.spark.*")
    java_import(self._gateway.jvm, 'java.util.*')

  def _getSC(self):
    return self._sc

  @property
  def sc(self):
    try:
      sparkContextWrapper = self._entry_point.sparkContextWrapper()
      sconf = sparkContextWrapper.getSparkConf(sys.argv[2])
      conf = SparkConf(_jvm = self._gateway.jvm, _jconf = sconf)
      jsc = sparkContextWrapper.getSparkContext(sys.argv[2])
      self._sc = SparkContext(jsc=jsc, gateway=self._gateway, conf=conf)
      self.sc = self._getSC()
      return self._sc

    except Exception:
      print(traceback.format_exc())
      err = self._entry_point.errorWrapper()
      err.set(sys.argv[2], traceback.format_exc())

  def _getSQLC(self):
    return self._sqlc

  @property
  def sqlc(self):
    try:
      sparkContextWrapper = self._entry_point.sparkContextWrapper()
      self._sqlc = SQLContext(self.sc, sparkContextWrapper.getSqlContext(sys.argv[2]))
      self.sqlc = self._getSQLC()
      return  self._sqlc

    except Exception:
      print(traceback.format_exc())
      err = self._entry_point.errorWrapper()
      err.set(sys.argv[2], traceback.format_exc())

  def _getHC(self):
    return self._hc

  @property
  def hc(self):
    try:
      sparkContextWrapper = self._entry_point.sparkContextWrapper()
      self._hc = HiveContext(self.sc, sparkContextWrapper.getHiveContext(sys.argv[2]))
      self.hc = self._getHC()
      return self._hc

    except Exception:
      print(traceback.format_exc())
      err = self._entry_point.errorWrapper()
      err.set(sys.argv[2], traceback.format_exc())

  @property
  def parameters(self):
    try:
      dataWrapper = self._entry_point.dataWrapper()
      self._parameters = dataWrapper.get(sys.argv[2])
      return self._parameters

    except Py4JJavaError:
      print("except Py4JJavaError")
      print(traceback.format_exc())
      err = self._entry_point.errorWrapper()
      err.set(sys.argv[2], traceback.format_exc())

    except Exception:
      print(traceback.format_exc())
      err = self._entry_point.errorWrapper()
      err.set(sys.argv[2], traceback.format_exc())

  def sendResult(self, result):
    try:
      dataWrapper = self._entry_point.dataWrapper()
      dataWrapper.set(sys.argv[2], result)

    except Py4JJavaError:
      print("except Py4JJavaError")
      print(traceback.format_exc())
      err = self._entry_point.errorWrapper()
      err.set(sys.argv[2], traceback.format_exc())

    except Exception:
      print(traceback.format_exc())
      err = self._entry_point.errorWrapper()
      err.set(sys.argv[2], traceback.format_exc())