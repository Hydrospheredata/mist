import pytest

from mistpy.decorators import *
from mistpy.executable_entry import is_mist_function


def test_int_arg():
    params = {'z': 10, 'y': 20}
    x = arg('z', type_hint=int)._decorate([], params)
    assert x == ([], params)

def test_int_arg_with_default():
    params = {}
    x = arg('z', type_hint=int, default = 42)._decorate([], params)
    assert x == ([], {'z': 42})

def test_hello_fn():
    @with_args(
       arg('name', type_hint=str)     
    )
    def hello_fn(name):
        return "Hello " + name

    assert hello_fn(name = 'Allie') == "Hello Allie"
    
def test_hello_fn2():
    @with_args(
       arg('greeting', type_hint=str),     
       arg('name', type_hint=str)     
    )
    def hello_fn2(greeting, name):
        return greeting + " " + name

    assert hello_fn2(name = 'Allie', greeting = 'Hello') == "Hello Allie"

def test_sys_args():
    @on_spark_context
    def sc1(sc):
        return sc

    @on_spark_session
    def sc2(sc):
        return sc

    @on_sql_context
    def sc3(sc):
        return sc

    @on_hive_context
    def sc4(sc):
        return sc

    @on_hive_session
    def sc5(sc):
        return sc

    @on_streaming_context
    def sc6(sc):
        return sc

    fns = [sc1, sc2, sc3, sc4, sc5, sc6]
    for f in fns:
        assert is_mist_function(f) == True
        assert f(42) == 42

def test_complex():
    @with_args(
       arg('a', type_hint=int),     
       arg('b', type_hint=str),
       arg('c', type_hint=list, default = [1,2,3])
    )
    @on_spark_context
    def complex(sc, a, b, c):
        return [sc, a, b, c]

    assert is_mist_function(complex) == True
    assert complex(42, a = 1, b = 'yoyo') == [42, 1, 'yoyo', [1, 2, 3]]
