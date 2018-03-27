# coding=utf-8

import argparse
import traceback
import types

from py4j.java_gateway import java_import, JavaGateway, GatewayClient

from mist.decorators import __complex_type
from mist.executable_entry import get_metadata


def to_scala_arg_type(type_hint, gateway):
    if type_hint is None:
        return gateway.jvm.mist.api.args.MAny.apply()

    if isinstance(type_hint, __complex_type):
        if issubclass(type_hint.container_type, list):
            arg_type = gateway.jvm.mist.api.args.MList.apply(to_scala_arg_type(type_hint.main_type, gateway))
        else:
            arg_type = gateway.jvm.mist.api.args.MAny.apply()
    else:
        if issubclass(type_hint, str):
            arg_type = gateway.jvm.mist.api.args.MString.apply()
        elif issubclass(type_hint, int):
            arg_type = gateway.jvm.mist.api.args.MInt.apply()
        elif issubclass(type_hint, float):
            arg_type = gateway.jvm.mist.api.args.MDouble.apply()
        elif issubclass(type_hint, bool):
            arg_type = gateway.jvm.mist.api.args.MBoolean.apply()

        elif issubclass(type_hint, list):
            arg_type = gateway.jvm.mist.api.args.MList.apply(
                gateway.jvm.mist.api.args.MAny.apply()
            )
        else:
            arg_type = gateway.jvm.mist.api.args.MAny.apply()

    return arg_type


def to_scala_internal_arg_info(tags, gateway):
    return gateway.jvm.mist.api.args.InternalArgument.apply(tags)


def to_scala_arg_info(arg_decorator_like, gateway):
    type_hint = arg_decorator_like.type_hint
    arg_type = to_scala_arg_type(type_hint, gateway)
    arg = gateway.jvm.mist.api.args.UserInputArgument.apply(arg_decorator_like.name, arg_type)
    return arg


def metadata_cmd(input_args):
    _client = GatewayClient(port=input_args.gateway_port)
    _gateway = JavaGateway(_client, auto_convert=True)
    _entry_point = _gateway.entry_point
    java_import(_gateway.jvm, 'java.util.*')
    java_import(_gateway.jvm, 'io.hydrosphere.mist.python.PythonUtils')
    error_wrapper = _entry_point.error()
    class_name = _entry_point.functionName()
    file_path = _entry_point.filePath()
    data_wrapper = _entry_point.data()

    try:
        with open(file_path) as file:
            code = compile(file.read(), file_path, "exec")
        user_job_module = types.ModuleType("<user_job>")
        exec(code, user_job_module.__dict__)

        module_entry = getattr(user_job_module, class_name)
        executable_entry = get_metadata(module_entry)
        internal_arg = to_scala_internal_arg_info(executable_entry.tags, _gateway)
        arg_infos = list(map(lambda a: to_scala_arg_info(a, _gateway), executable_entry.args_info))
        arg_infos.append(internal_arg)
        data_wrapper.set(_gateway.jvm.PythonUtils.toSeq(arg_infos))
    except Exception:
        error_wrapper.set(traceback.format_exc())


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--gateway-port', type=int)
    args = parser.parse_args()
    metadata_cmd(args)
