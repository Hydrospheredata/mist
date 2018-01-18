from abc import abstractmethod

from decorators import SPARK_CONTEXT, SPARK_SESSION, HIVE_SESSION, HIVE_CONTEXT, SQL_CONTEXT
from decorators import tags as dec_tags
from mist.mist_job import WithPublisher
from mist_job import WithHiveSupport, WithSQLSupport, MistJob
from inspect import isfunction


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


class ClassEntry(ExecutableEntry):

    def __init__(self, class_):
        type_choice = SPARK_CONTEXT
        tags = set()
        try:
            from pyspark.sql import SparkSession
            if issubclass(class_, WithSQLSupport):
                type_choice = SPARK_SESSION
                tags = dec_tags.sql
            if issubclass(class_, WithHiveSupport):
                type_choice = HIVE_SESSION
                tags = dec_tags.hive
        except ImportError:
            if issubclass(class_, WithSQLSupport):
                type_choice = SQL_CONTEXT
                tags = dec_tags.sql
            if issubclass(class_, WithHiveSupport):
                type_choice = HIVE_CONTEXT
                tags = dec_tags.hive
        with_publisher = False
        if issubclass(class_, WithPublisher):
            with_publisher = True

        self._with_publisher = with_publisher
        self._class = class_
        super(ClassEntry, self).__init__(type_choice, tags=tags)

    @property
    def with_publisher(self):
        return self._with_publisher

    def invoke(self, context_wrapper, params):
        instance = self._class()
        instance.setup(context_wrapper)
        return instance.execute(**params)


def is_mist_function(fn):
    return hasattr(fn, 'fn_tags') and hasattr(fn, 'type_choice') and hasattr(fn, 'args_def')


def get_metadata(fn_or_class):
    if isfunction(fn_or_class):
        if is_mist_function(fn_or_class):
            return FunctionEntry(fn_or_class)
        else:
            raise Exception(fn_or_class + ' is not a mist function')

    if issubclass(fn_or_class, MistJob):
        return ClassEntry(fn_or_class)
    else:
        raise Exception(str(fn_or_class) + ' is not a subclass of MistJob')
