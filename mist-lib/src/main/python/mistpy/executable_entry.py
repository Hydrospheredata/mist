import inspect
import sys
import importlib
import types
from abc import abstractmethod
from collections import namedtuple
from inspect import isfunction

from mistpy.tags import SPARK_CONTEXT, SPARK_SESSION, HIVE_SESSION, HIVE_CONTEXT, SQL_CONTEXT
from mistpy.tags import tags as dec_tags


class ExecutableEntry(object):

    def __init__(self, type_choice, args=None, tags=None):
        self._type_choice = type_choice
        if args is None:
            args = []

        if tags is None:
            tags = set()

        self._args = args
        self._tags = tags

    @property
    def args_info(self):
        return self._args

    @property
    def selected_spark_argument(self):
        return self._type_choice

    @property
    def tags(self):
        return self._tags

    @abstractmethod
    def invoke(self, context_wrapper, params):
        pass


class FunctionEntry(ExecutableEntry):
    def __init__(self, fn):
        self._fn = fn
        if hasattr(fn, 'args_def'):
            args = fn.args_def
        else:
            args = []
        type_choice = fn.type_choice
        _tags = fn.fn_tags
        super(FunctionEntry, self).__init__(type_choice, args, _tags)

    def invoke(self, context_wrapper, params):
        return self._fn(context_wrapper, **params)


ArgInfo = namedtuple('ArgInfo', ['name', 'type_hint'])


def extract_args_from_method_py2(method):
    args_spec = inspect.getargspec(method)
    user_args = args_spec.args[1:]
    return list(map(lambda name: ArgInfo(name, None), user_args))


def extract_args_from_method_py3(method):
    sign = inspect.signature(method)
    result = []
    user_params = list(sign.parameters.items())[1:]
    for k, v in user_params:
        result.append(ArgInfo(k, None))
    return result


def extract_args_from_method(method):
    if sys.version_info[0] == 2:
        args = extract_args_from_method_py2(method)
    else:
        args = extract_args_from_method_py3(method)
    return args


def is_mist_function(fn):
    return hasattr(fn, 'fn_tags') and hasattr(fn, 'type_choice')


def get_metadata(fn):
    if isfunction(fn):
        if is_mist_function(fn):
            return FunctionEntry(fn)
        else:
            raise Exception(fn.__name__ + ' is not a mist function')

def load_egg_entry(path, fn_name):
    sys.path.append(path)
    splitted = fn_name.split('.')
    module_path = '.'.join(splitted[:-1])
    fn_attr = splitted[-1]

    module = importlib.import_module(module_path)
    return getattr(module, fn_attr)

def load_one_file(path, fn_name):
    with open(path) as file:
        code = compile(file.read(), path, "exec")
    user_job_module = types.ModuleType("<user_job>")
    exec(code, user_job_module.__dict__)
    return getattr(user_job_module, fn_name)

def load_entry_object(path, fn_name):
    if (path.endswith(".egg")):
        return load_egg_entry(path, fn_name)
    else:
        return load_one_file(path, fn_name)

def load_entry(path, fn_name):
    module_entry = load_entry_object(path, fn_name)
    return get_metadata(module_entry)