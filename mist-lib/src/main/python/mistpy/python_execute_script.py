# coding=utf-8

import sys, traceback, argparse
from py4j.java_gateway import java_import, JavaGateway, GatewayClient
from py4j.java_collections import JavaMap, JavaList

from pyspark.sql.types import *

from mistpy.executable_entry import load_entry
from mistpy.context_wrapper import ContextWrapper
from mistpy.decorators import SPARK_CONTEXT, SPARK_SESSION, SPARK_STREAMING, HIVE_CONTEXT, HIVE_SESSION, SQL_CONTEXT


def to_python_types(any):
    python_any = any
    if isinstance(any, JavaMap):
        python_any = dict()
        for key, value in any.items():
            python_any[key] = to_python_types(value)
    elif isinstance(any, JavaList):
        python_any = list()
        for i, value in enumerate(any):
            python_any.insert(i, to_python_types(value))
    return python_any


def initialized_context_value(_gateway, context_wrapper, selected_spark_argument):
    if selected_spark_argument == SPARK_CONTEXT:
        argument = context_wrapper.context
    elif selected_spark_argument == SPARK_SESSION:
        context_wrapper.set_session(_gateway)
        argument = context_wrapper.session
    elif selected_spark_argument == HIVE_SESSION:
        context_wrapper.set_hive_session(_gateway)
        argument = context_wrapper.session
    elif selected_spark_argument == HIVE_CONTEXT:
        context_wrapper.set_hive_context(_gateway)
        argument = context_wrapper.hive_context
    elif selected_spark_argument == SPARK_STREAMING:
        context_wrapper.set_streaming_context(_gateway)
        argument = context_wrapper.streaming_context
    elif selected_spark_argument == SQL_CONTEXT:
        context_wrapper.set_sql_context(_gateway)
        argument = context_wrapper.sql_context
    else:
        raise Exception('Unknown spark argument type: ' + selected_spark_argument)
    return argument


def execution_cmd(args):
    _client = GatewayClient(port=args.gateway_port)
    _gateway = JavaGateway(_client, auto_convert=True)
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
    error_wrapper = _entry_point.error()
    path = configuration_wrapper.path()
    fn_name = configuration_wrapper.className()
    parameters = configuration_wrapper.parameters()

    data_wrapper = _entry_point.data()


    try:
        executable_entry = load_entry(path, fn_name)
        selected_spark_argument = executable_entry.selected_spark_argument

        argument = initialized_context_value(_gateway, context_wrapper, selected_spark_argument)
        result = executable_entry.invoke(argument, to_python_types(parameters))
        data_wrapper.set(result)

    except Exception:
        error_wrapper.set(traceback.format_exc())
