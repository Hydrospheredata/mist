from abc import abstractmethod

SPARK_CONTEXT = 'sp_context'
SPARK_SESSION = 'sp_session'
SQL_CONTEXT = 'sql_context'
HIVE_CONTEXT = 'hive_context'
HIVE_SESSION = 'hive_session'
SPARK_STREAMING = 'streaming'

TYPE_CHOICES = (SPARK_CONTEXT, SPARK_SESSION, SQL_CONTEXT, HIVE_CONTEXT, HIVE_SESSION, SPARK_STREAMING)


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
    def __init__(self, name, type_hint, callback=None):
        self.name = name
        self.type_hint = type_hint
        self.callback = callback

    @abstractmethod
    def _decorate(self, fn_args, fn_kwargs):
        return fn_args, fn_kwargs

    def __call__(self, fn):
        def wrapper(*args, **kwargs):
            new_args, new_kwargs = self._decorate(args, kwargs)
            return fn(*new_args, **new_kwargs)

        return wrapper


class NamedArg(AbstractArg):
    def _decorate(self, fn_args, fn_kwargs):
        new_kw = dict(fn_kwargs)
        args = list(fn_args)
        if self.name not in fn_kwargs:
            if len(args) < 2:  # one for context
                raise BadParameterException("not found argument in kwargs by %s and by args order".format(self.name))
            else:
                new_kw[self.name] = args[1]
                args.pop(1)
        return args, new_kw


class NamedArgWithDefault(AbstractArg):
    def __init__(self, name, type_hint, callback=None, default=None):
        super(NamedArgWithDefault, self).__init__(name, type_hint, callback)
        self.default = default

    def _decorate(self, fn_args, fn_kwargs):
        if self.name in fn_kwargs:
            return fn_args, fn_kwargs
        else:
            fn_kwargs[self.name] = self.default
            return fn_args, fn_kwargs


class OptionalArg(AbstractArg):
    def __init__(self, name, type_hint, callback):
        super(OptionalArg, self).__init__(name, type_hint, callback)

    def _decorate(self, fn_args, fn_kwargs):
        new_kwargs = dict(fn_kwargs)
        if self.name not in fn_kwargs:
            new_kwargs[self.name] = None
        return fn_args, fn_kwargs


def arg(name, type_hint, default=None, callback=None, optional=False):
    if optional:
        return OptionalArg(name, type_hint, callback)
    if default is not None:
        return NamedArgWithDefault(name, type_hint, callback, default)
    return NamedArg(name, type_hint, callback)


def opt_arg(name, type_hint):
    return arg(name, type_hint, optional=True)


class _tags(object):
    def __init__(self):
        self.sql = {'sql'}
        self.hive = {'hive', 'sql'}
        self.streaming = {'streaming'}


tags = _tags()
on_spark_context = SystemArg(SPARK_CONTEXT)
on_spark_session = SystemArg(SPARK_SESSION, tags.sql)
on_sql_context = SystemArg(SQL_CONTEXT, tags.sql)
on_hive_context = SystemArg(HIVE_CONTEXT, tags.hive)
on_hive_session = SystemArg(HIVE_SESSION, tags.hive)
on_streaming_context = SystemArg(SPARK_STREAMING, tags.streaming)


def with_args(*args_decorator):
    def _call(f):
        def _wraps(*args, **kwargs):
            for arg_decorator in args_decorator:
                fn = arg_decorator(f)
            return fn(*args, **kwargs)

        _wraps.args_def = args_decorator
        _wraps.fn_tags = f.fn_tags
        _wraps.type_choice = f.type_choice
        return _wraps

    return _call
