# coding=utf-8

import argparse
import traceback

from py4j.java_gateway import java_import, JavaGateway, GatewayClient

from mistpy.decorators import __complex_type
from mistpy.executable_entry import load_entry


def to_scala_arg_type(type_hint, gateway):
    if type_hint is None:
        return gateway.jvm.PythonUtils.anyType()

    if isinstance(type_hint, __complex_type):
        if issubclass(type_hint.container_type, list):
            underlying = to_scala_arg_type(type_hint.main_type, gateway)
            arg_type = gateway.jvm.PythonUtils.listType(underlying)
        else:
            arg_type = gateway.jvm.PythonUtils.anyType()
    else:
        if issubclass(type_hint, str):
            arg_type = gateway.jvm.PythonUtils.strType()
        elif issubclass(type_hint, int):
            arg_type = gateway.jvm.PythonUtils.intType()
        elif issubclass(type_hint, float):
            arg_type = gateway.jvm.PythonUtils.doubleType()
        elif issubclass(type_hint, bool):
            arg_type = gateway.jvm.PythonUtils.boolType()

        elif issubclass(type_hint, list):
            arg_type = gateway.jvm.PythonUtils.listType(gateway.jvm.PythonUtils.anyType())
        else:
            arg_type = gateway.jvm.PythonUtils.anyType()

    return arg_type


def to_scala_internal_arg_info(tags, gateway):
    return gateway.jvm.PythonUtils.systemArg(tags)


def to_scala_arg_info(arg_decorator_like, gateway):
    type_hint = arg_decorator_like.type_hint
    arg_type = to_scala_arg_type(type_hint, gateway)
    if arg_decorator_like.is_optional:
        arg_type = gateway.jvm.PythonUtils.optType(arg_type)
    arg = gateway.jvm.PythonUtils.userArg(arg_decorator_like.name, arg_type)
    return arg


def metadata_cmd(input_args):
    _client = GatewayClient(port=input_args.gateway_port)
    _gateway = JavaGateway(_client, auto_convert=True)
    _entry_point = _gateway.entry_point
    java_import(_gateway.jvm, 'java.util.*')
    java_import(_gateway.jvm, 'io.hydrosphere.mist.python.PythonUtils')
    error_wrapper = _entry_point.error()
    fn_name = _entry_point.functionName()
    file_path = _entry_point.filePath()
    data_wrapper = _entry_point.data()

    try:
        executable_entry = load_entry(file_path, fn_name)
        internal_arg = to_scala_internal_arg_info(executable_entry.tags, _gateway)
        arg_infos = list(map(lambda a: to_scala_arg_info(a, _gateway), executable_entry.args_info))
        arg_infos.append(internal_arg)
        data_wrapper.set(_gateway.jvm.PythonUtils.toSeq(arg_infos))
    except Exception:
        error_wrapper.set(traceback.format_exc())
