from abc import abstractmethod

from mistpy.tags import TYPE_CHOICES, SPARK_CONTEXT, SPARK_SESSION, SQL_CONTEXT, HIVE_CONTEXT, HIVE_SESSION, SPARK_STREAMING
from mistpy.tags import tags as dec_tags


class BadParameterException(Exception):
    def __init(self, name, type_hint, additional_info=""):
        self.name = name
        self.type_hint = type_hint
        self.additional_info = additional_info

    def __str__(self):
        return "Bad parameter {} of type {}: {}".format(self.name, self.type_hint, self.additional_info)


class SystemArg(object):

    def __init__(self, type_choice, tags=set()):
        if type_choice in TYPE_CHOICES:
            self.type_choice = type_choice
        else:
            raise AttributeError("wrong value of type choice %s".format(type_choice))
        self.tags = tags

    def __call__(self, fn):
        def wrapper(*args, **kwargs):
            args_len = len(args)
            if args_len != 1:
                message = "it is possible to pass only one argument as python arg, but passed %s, " \
                          "all user defined argument passed as kwargs".format(args_len)
                raise BadParameterException(message)
            return fn(args[0], **kwargs)

        wrapper.type_choice = self.type_choice
        wrapper.fn_tags = self.tags
        return wrapper


class AbstractArg(object):
    def __init__(self, name, type_hint, is_optional):
        self.name = name
        self.type_hint = type_hint
        self.is_optional = is_optional

    @abstractmethod
    def _decorate(self, fn_args, fn_kwargs):
        return fn_args, fn_kwargs

    def __call__(self, fn):
        def wrapper(*args, **kwargs):
            new_args, new_kwargs = self._decorate(args, kwargs)
            return fn(*new_args, **new_kwargs)

        return wrapper


class NamedArg(AbstractArg):

    def __init__(self, name, type_hint):
        super(NamedArg, self).__init__(name, type_hint, False)

    def _decorate(self, fn_args, fn_kwargs):
        new_kw = dict(fn_kwargs)
        args = list(fn_args)
        if self.name not in fn_kwargs:
            # TODO < 2 ?
            if len(args) < 2:  # one for context
                raise BadParameterException("not found argument in kwargs by %s and by args order".format(self.name))
            else:
                new_kw[self.name] = args[1]
                args.pop(1)
        return args, new_kw


class NamedArgWithDefault(AbstractArg):
    def __init__(self, name, type_hint, default=None):
        super(NamedArgWithDefault, self).__init__(name, type_hint, True)
        self.default = default

    def _decorate(self, fn_args, fn_kwargs):
        if self.name in fn_kwargs:
            return fn_args, fn_kwargs
        else:
            fn_kwargs[self.name] = self.default
            return fn_args, fn_kwargs


class OptionalArg(AbstractArg):
    def __init__(self, name, type_hint):
        super(OptionalArg, self).__init__(name, type_hint, True)

    def _decorate(self, fn_args, fn_kwargs):
        new_kwargs = dict(fn_kwargs)
        if self.name not in fn_kwargs:
            new_kwargs[self.name] = None
        return fn_args, fn_kwargs


def arg(name, type_hint, default=None, optional=False):
    if optional:
        return OptionalArg(name, type_hint)
    if default is not None:
        return NamedArgWithDefault(name, type_hint, default)
    return NamedArg(name, type_hint)


def opt_arg(name, type_hint):
    return arg(name, type_hint, optional=True)


class __complex_type(object):
    def __init__(self, container_type, main_type):
        self.container_type = container_type
        self.main_type = main_type


def list_type(main_type):
    return __complex_type(list, main_type)


def with_args(*args_decorator):
    def _call(f):
        def _wraps(*args, **kwargs):
            for arg_decorator in args_decorator:
                fn = arg_decorator(f)
            return fn(*args, **kwargs)
        
        _wraps.__dict__.update(f.__dict__)
        _wraps.args_def = args_decorator
        return _wraps

    return _call


on_spark_context = SystemArg(SPARK_CONTEXT)
on_spark_session = SystemArg(SPARK_SESSION, dec_tags.sql)
on_sql_context = SystemArg(SQL_CONTEXT, dec_tags.sql)
on_hive_context = SystemArg(HIVE_CONTEXT, dec_tags.hive)
on_hive_session = SystemArg(HIVE_SESSION, dec_tags.hive)
on_streaming_context = SystemArg(SPARK_STREAMING, dec_tags.streaming)
