---
layout: docs
title: "Python library API"
permalink: lib_python.html
position: 6
---
## Python library API

Definitions:

*Mist Function* is a functional framework that defines particular Spark calculation. Mist Function is a deployable unit for Mist proxy.
 
*Job* - a Spark job triggered by Mist Function.

Mist Library provides a decorator based DSL for Mist Functions that could be deployed and executed in Mist.
`python_pi_example.py`:
```python
from mistpy.decorators import *
import random

@with_args(
    arg("samples", type_hint=int)
)
@on_spark_context
def hello_mist(sc, samples):
    def inside(p):
        x, y = random.random(), random.random()
        return x * x + y * y < 1

    count = sc.parallelize(xrange(0, samples)) \
        .filter(inside).count()

    pi = 4.0 * count / samples
    return {'result': pi}
```

#### Build

`setup.py`:
```python
import os
from setuptools import setup

setup(
    name='hello-mist',
    install_requires=["pyspark==2.3.0", "mistpy=={{ site.version }}"]
)
```

#### Overview

Speaking generally - to write you own mist function declaration using python you need to declare context type
and input arguments

#### Contexts

Mist provides managed Spark Contexts, so developer does not care about context's lifecycle and settings.
In python library we use special context decorators to inject Spark Context into function.
For exmaple: if a function is marked using `on_spark_context` it means that user wants to receive a `pyspark.SparkContext`
instance into it. Contexts instances is always passed as a first argument:
```python
from mist.decorators import *

@on_spark_context
def my_func(sc): 
    pass
```

All context decorators:
- `on_spark_context` - `pyspark.SparkContext`
- `on_spark_session` - `pyspark.sql.SparkSession`
- `on_hive_session`  - `pyspark.sql.SparkSession` with enabled Hive support
- `on_streaming_context` - `pyspark.streaming.StreamingContext`
- `on_sql_context`   - `pyspark.sql.SQLContext`
- `on_hive_context`  - `pyspark.sql.HiveContext`

#### Arguments

Input arguments can be declared with inside `with_args` decorator:
```python

@with_args(
  arg('first', type_hint=int)
)
@on_spark_context
def one_arg_fn(sc, first):
    ....

@with_arg(
  arg('first', type_hint=int),
  arg('second', type_hint=int)
)
@on_spark_context
def two_args_fn(sc, first, second):
    ....
```

Arguments can be declared using following methods:
- `arg(name, type_hint, default = None)`
- `opt_arg(name, type_hint)`

Where:
- `name` is argument key in input json
- `type_hint` is used to annotate argument type.
  It accepts default primitive types: `int`, `str`, `float`, `bool`.
  For lists there is `list_type(type)` function:
  ```python
  arg('list_of_ints', type_hint=list_type(int))
  ```
- `default` is used to provide default value for argument that makes possible to skip it in input data
  ```python
  arg('list_of_ints', type_hint=list_type(int), default = [1,2,3,4,5])
  ```

`with_args` is optional for usage, if you don't need any argument expect spark context you can skip it

#### Logging

To be able to log and see what's going on on job side from mist-ui you to use log4j logger:
```python
@on_spark_context
def example(sc):
    log4jLogger = sc._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger(__name__)
    logger.info("Hello!")
    ...
```

### Python versions

Python version could be explicitly specified with spark configurations in default mist context for function.
Mist respects `spark.pyspark.python` and `spark.pyspark.driver.python` congurations.
For example in `mist-cli` configuration:
`context.conf`:
```
model = Context
name = py3
data {
  spark-conf {
    spark.pyspark.python = "python3"
  }
}
```

`my-function.conf`:
```
model = Function
name =  mypy3function 
data {
    context = py3
    ...
}
```

