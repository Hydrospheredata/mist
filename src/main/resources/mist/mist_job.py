# coding=utf-8

from abc import ABCMeta, abstractmethod

class ContextSupport:
    __metaclass__ = ABCMeta

    def __init__(self):
        pass

    context = None

    @abstractmethod
    def setup(self, context_wrapper):
        self.context = context_wrapper.context

class MistJob(ContextSupport):
    __metaclass__ = ABCMeta

    @abstractmethod
    def execute(self, *args):
        raise NotImplementedError()

    def setup(self, context_wrapper):
        super(MistJob, self).setup(context_wrapper)

class WithSQLSupport(ContextSupport):
    __metaclass__ = ABCMeta

    sql_context = None
    session = None

    @abstractmethod
    def setup(self, context_wrapper):
        super(WithSQLSupport, self).setup(context_wrapper)
        try:
            from pyspark.sql import SparkSession
            self.session = context_wrapper.session
        except ImportError:
            self.sql_context = context_wrapper.sql_context

class WithHiveSupport(ContextSupport):
    __metaclass__ = ABCMeta

    hive_context = None
    session = None

    @abstractmethod
    def setup(self, context_wrapper):
        super(WithHiveSupport, self).setup(context_wrapper)
        try:
            from pyspark.sql import SparkSession
            self.session = context_wrapper.session
        except ImportError:
            self.hive_context = context_wrapper.hive_context

class WithPublisher(ContextSupport):
    __metaclass__ = ABCMeta

    publisher = None

    @abstractmethod
    def setup(self, context_wrapper):
        self.publisher = context_wrapper.publisher

class WithStreamingContext(ContextSupport):
    __metaclass__ = ABCMeta

    streaming_context = None

    @abstractmethod
    def setup(self, context_wrapper):
        super(WithStreamingContext, self).setup(context_wrapper)
        self.streaming_context = context_wrapper.streaming_context
