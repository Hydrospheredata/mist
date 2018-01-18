# coding=utf-8

import sys, traceback, types, argparse
from py4j.java_gateway import java_import, JavaGateway, GatewayClient
from py4j.java_collections import JavaMap, JavaList

from pyspark.sql.types import *

from mist.executable_entry import get_metadata, ClassEntry
from mist.context_wrapper import ContextWrapper
from mist.decorators import SPARK_CONTEXT, SPARK_SESSION, SPARK_STREAMING, HIVE_CONTEXT, HIVE_SESSION, SQL_CONTEXT


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
    error_wrapper = _entry_point.errorWrapper()
    path = configuration_wrapper.path()
    class_name = configuration_wrapper.className()
    parameters = configuration_wrapper.parameters()

    data_wrapper = _entry_point.dataWrapper()

    try:
        with open(path) as f:
            code = compile(f.read(), path, "exec")
        user_job_module = types.ModuleType("<user_job>")
        exec code in user_job_module.__dict__

        module_entry = getattr(user_job_module, class_name)
        executable_entry = get_metadata(module_entry)
        selected_spark_argument = executable_entry.selected_spark_argument

        argument = initialized_context_value(_gateway, context_wrapper, selected_spark_argument)

        if isinstance(executable_entry, ClassEntry):
            if executable_entry.with_publisher:
                context_wrapper.init_publisher(_gateway)
            result = executable_entry.invoke(context_wrapper, to_python_types(parameters))
        else:
            result = executable_entry.invoke(argument, to_python_types(parameters))

        data_wrapper.set(result)

    except Exception:
        error_wrapper.set(traceback.format_exc())


def metadata_cmd(args):
    # TODO:
    pass
    # return get_metadata(args.fn_name)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()
    exec_cmd = subparsers.add_parser('execution')
    exec_cmd.add_argument('--gateway-port', type=int)
    exec_cmd.set_defaults(func=execution_cmd)
    metadata_cmd = subparsers.add_parser('metadata')
    metadata_cmd.add_argument('--fn-name', type=str)
    metadata_cmd.set_defaults(func=metadata_cmd)
    args = parser.parse_args()
    args.func(args)

