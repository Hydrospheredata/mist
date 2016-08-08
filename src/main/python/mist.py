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
#from pyspark.sql import SQLContext, HiveContext, SchemaRDD, Row

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
      sconf = sparkContextWrapper.getSparkConf()
      conf = SparkConf(_jvm = self._gateway.jvm, _jconf = sconf)
      jsc = sparkContextWrapper.getSparkContext()
      self._sc = SparkContext(jsc=jsc, gateway=self._gateway, conf=conf)
      self.sc = self._getSC()
      return self._sc

    except Exception:
      print(traceback.format_exc())
      err = self._entry_point.errorWrapper()
      err.set(traceback.format_exc())

  def _getSQLC(self):
    return self._sqlc

  @property
  def sqlc(self):
    try:
      from pyspark.sql import SQLContext, SchemaRDD, Row
      sparkContextWrapper = self._entry_point.sparkContextWrapper()
      self._sqlc = SQLContext(self.sc, sparkContextWrapper.getSqlContext())
      self.sqlc = self._getSQLC()
      return  self._sqlc

    except Exception:
      print(traceback.format_exc())
      err = self._entry_point.errorWrapper()
      err.set(traceback.format_exc())

  def _getHC(self):
    return self._hc

  @property
  def hc(self):
    try:
      from pyspark.sql import SQLContext, HiveContext, SchemaRDD, Row
      sparkContextWrapper = self._entry_point.sparkContextWrapper()
      self._hc = HiveContext(self.sc, sparkContextWrapper.getHiveContext())
      self.hc = self._getHC()
      return self._hc

    except Exception:
      print(traceback.format_exc())
      err = self._entry_point.errorWrapper()
      err.set(traceback.format_exc())

  def _getSS(self):
    return self._ss

  @property
  def ss(self):
    try:
      from pyspark.sql import SparkSession
      sparkContextWrapper = self._entry_point.sparkContextWrapper()
      self._ss = SparkSession(self.sc, sparkContextWrapper.getSparkSession())
      self.ss = self._getSS()
      return self._ss

    except Exception:
      print(traceback.format_exc())
      err = self._entry_point.errorWrapper()
      err.set(traceback.format_exc())


  @property
  def parameters(self):
    try:
      dataWrapper = self._entry_point.dataWrapper()
      self._parameters = dataWrapper.get()
      return self._parameters

    except Py4JJavaError:
      print("except Py4JJavaError")
      print(traceback.format_exc())
      err = self._entry_point.errorWrapper()
      err.set(traceback.format_exc())

    except Exception:
      print(traceback.format_exc())
      err = self._entry_point.errorWrapper()
      err.set(traceback.format_exc())

  def sendResult(self, result):
    try:
      dataWrapper = self._entry_point.dataWrapper()
      dataWrapper.set(result)

    except Py4JJavaError:
      print("except Py4JJavaError")
      print(traceback.format_exc())
      err = self._entry_point.errorWrapper()
      err.set(traceback.format_exc())

    except Exception:
      print(traceback.format_exc())
      err = self._entry_point.errorWrapper()
      err.set(traceback.format_exc())