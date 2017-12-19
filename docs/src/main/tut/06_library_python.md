---
layout: docs
title: "Python library API"
position: 6
---
## Python library API

Import [mist](https://github.com/Hydrospheredata/mist/tree/master/src/main/resources/mist), extend MistJob class and implement method `def execute(self)`: 

```python
from mist.mist_job import *

class MyPythonMistJob(MistJob):
    
    def execute(self):
        rdd = self.context.parallelize()
        ...
        return dict()
```

Add `WithSQLSupport` and `WithHiveSupport` to get `SparkSession` instance in your subclass.

```python
from mist.mist_job import *

class MySessionJob(MistJob, WithSQLSupport, WithHiveSupport):
    
    def execute(self):
        data_frame = self.session.read("file.parquet")
        ...
        return dict()
```

Or for spark < 2.0.0:

```python
from mist.mist_job import *

class MyOldSparkJob(MistJob, WithSQLSupport, WithHiveSupport):
    
    def execute(self):
        data_frame = self.sql_context.read("file.parquet") # or self.hive_context.read("file.parquet")
        ...
        return dict()
```
