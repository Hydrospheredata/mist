import inspect
import sys
from abc import abstractmethod
from collections import namedtuple
from inspect import isfunction

from tags import SPARK_CONTEXT, SPARK_SESSION, HIVE_SESSION, HIVE_CONTEXT, SQL_CONTEXT
from tags import tags as dec_tags


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
        args = fn.args_def
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
    return hasattr(fn, 'fn_tags') and hasattr(fn, 'type_choice') and hasattr(fn, 'args_def')


def get_metadata(fn_or_class):
    if isfunction(fn_or_class):
        if is_mist_function(fn_or_class):
            return FunctionEntry(fn_or_class)
        else:
            raise Exception(str(fn_or_class) + ' is not a mist function')
