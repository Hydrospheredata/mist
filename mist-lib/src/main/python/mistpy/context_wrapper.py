# coding=utf-8 

from pyspark.conf import SparkConf
from pyspark.context import SparkContext

class ContextWrapper(object):

    def __init__(self):
        pass

    def set_context(self, java_gateway):
        spark_context_wrapper = java_gateway.entry_point.sparkContextWrapper()
        j_spark_conf = spark_context_wrapper.sparkConf()
        p_spark_conf = SparkConf(_jvm=java_gateway.jvm, _jconf=j_spark_conf)
        j_spark_context = spark_context_wrapper.javaContext()
        self._context = SparkContext(jsc=j_spark_context, gateway=java_gateway, conf=p_spark_conf)

    def set_sql_context(self, java_gateway):
        from pyspark.sql import SQLContext
        spark_context_wrapper = java_gateway.entry_point.sparkContextWrapper()
        self._sql_context = SQLContext(self._context, sparkSession = spark_context_wrapper.sparkSession(False), jsqlContext = spark_context_wrapper.sqlContext())

    def set_hive_context(self, java_gateway):
        from pyspark.sql import HiveContext
        spark_context_wrapper = java_gateway.entry_point.sparkContextWrapper()
        self._hive_context = HiveContext(self._context, spark_context_wrapper.hiveContext())

    def set_session(self, java_gateway):
        from pyspark.sql import SparkSession
        self._session = SparkSession.builder.config(conf=self._context.getConf()).getOrCreate()

    def set_hive_session(self, java_gateway):
        from pyspark.sql import SparkSession
        self._session = SparkSession.builder.config(conf=self._context.getConf()).enableHiveSupport().getOrCreate()

    def set_streaming_context(self, java_gateway):
        from pyspark.streaming import StreamingContext
        spark_context_wrapper = java_gateway.entry_point.sparkContextWrapper()
        self._streaming_context = StreamingContext(self._context, java_gateway.entry_point.sparkStreamingWrapper().getDurationSeconds())

    @property
    def context(self):
        return self._context

    @property
    def sql_context(self):
        return self._sql_context

    @property
    def hive_context(self):
        return self._hive_context

    @property
    def session(self):
        return self._session

    @property
    def streaming_context(self):
        return self._streaming_context
