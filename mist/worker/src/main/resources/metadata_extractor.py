# coding=utf-8

import sys, traceback, types, argparse
from py4j.java_gateway import java_import, JavaGateway, GatewayClient
from py4j.java_collections import JavaMap, JavaList

from pyspark.sql.types import *

from mist.executable_entry import get_metadata, ClassEntry
from mist.context_wrapper import ContextWrapper
from mist.decorators import SPARK_CONTEXT, SPARK_SESSION, SPARK_STREAMING, HIVE_CONTEXT, HIVE_SESSION, SQL_CONTEXT


def execution_cmd(args):
    _client = GatewayClient(port=args.gateway_port)
    _gateway = JavaGateway(_client, auto_convert=True)
    _entry_point = _gateway.entry_point
    java_import(_gateway.jvm, 'java.util.*')

    configuration_wrapper = _entry_point.data()
    error_wrapper = _entry_point.error()
    class_name = entry_point.functionName()
    file_path = entry_point.filePath()
    data_wrapper = _entry_point.dataWrapper()

    try:
        with open(file_path) as file:
            code = compile(file.read(), path, "exec")
        user_job_module = types.ModuleType("<user_job>")
        exec(code, user_job_module.__dict__)

        module_entry = getattr(user_job_module, class_name)
        executable_entry = get_metadata(module_entry)
        selected_spark_argument = executable_entry.selected_spark_argument
        data_wrapper.set(executable_entry.)

    except Exception:
        error_wrapper.set(traceback.format_exc())


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--gateway-port', type=int)
    args = parser.parse_args()
    execution_cmd(args)

