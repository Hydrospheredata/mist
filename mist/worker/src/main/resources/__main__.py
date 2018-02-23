# coding=utf-8

import py4j.java_gateway
import pyspark
import sys, getopt, traceback, json, re, types

from py4j.java_gateway import java_import, JavaGateway, GatewayClient
from py4j.java_collections import SetConverter, MapConverter, ListConverter, JavaMap, JavaList
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

from mist.mist_job import *
from mist.context_wrapper import ContextWrapper

def to_python_types(any):
    python_any = any
    if isinstance(any, JavaMap):
        python_any = dict()
        for key, value in any.iteritems():
            python_any[key] = to_python_types(value)
    elif isinstance(any, JavaList):
        python_any = list()
        for i, value in enumerate(any):
            python_any.insert(i, to_python_types(value))
    return python_any


_client = GatewayClient(port=int(sys.argv[1]))
_gateway = JavaGateway(_client, auto_convert = True)
_entry_point = _gateway.entry_point

java_import(_gateway.jvm, "org.apache.spark.SparkContext")
java_import(_gateway.jvm, "org.apache.spark.SparkEnv")
java_import(_gateway.jvm, "org.apache.spark.SparkConf")
java_import(_gateway.jvm, "org.apache.spark.streaming.*")
java_import(_gateway.jvm, "org.apache.spark.streaming.api.java.*")
java_import(_gateway.jvm, "org.apache.spark.streaming.api.python.*")
java_import(_gateway.jvm, "org.apache.spark.api.java.*")
java_import(_gateway.jvm, "org.apache.spark.api.python.*")
java_import(_gateway.jvm, "org.apache.spark.mllib.api.python.*")
java_import(_gateway.jvm, "org.apache.spark.*")
java_import(_gateway.jvm, "org.apache.spark.sql.*")
java_import(_gateway.jvm, 'java.util.*')

context_wrapper = ContextWrapper()
context_wrapper.set_context(_gateway)

configuration_wrapper = _entry_point.configurationWrapper()
error_wrapper = _entry_point.errorWrapper()
path = configuration_wrapper.path()
class_name = configuration_wrapper.className()
parameters = configuration_wrapper.parameters()

data_wrapper = _entry_point.dataWrapper()

try:
    with open(path) as file:
        code = compile(file.read(), path, "exec")
    user_job_module = types.ModuleType("<user_job>")
    exec(code, user_job_module.__dict__)

    class_ = getattr(user_job_module, class_name)
    if not issubclass(class_, MistJob):
        raise Exception(class_name + " is not a subclass of MistJob")

    instance = class_()

    try:
        from pyspark.sql import SparkSession
        if issubclass(class_, WithSQLSupport):
            context_wrapper.set_session(_gateway)
        if issubclass(class_, WithHiveSupport):
            context_wrapper.set_hive_session(_gateway)
    except ImportError:
        if issubclass(class_, WithSQLSupport):
            context_wrapper.set_sql_context(_gateway)
        if issubclass(class_, WithHiveSupport):
            context_wrapper.set_hive_context(_gateway)

    if issubclass(class_, WithPublisher):
        context_wrapper.init_publisher(_gateway)

    if issubclass(class_, WithStreamingContext):
        context_wrapper.set_streaming_context(_gateway)

    instance.setup(context_wrapper)
    # TODO: train/serve
    result = instance.execute(**to_python_types(parameters))
    data_wrapper.set(result)
except Exception:
    error_wrapper.set(traceback.format_exc())
