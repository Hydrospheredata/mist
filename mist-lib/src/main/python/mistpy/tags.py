SPARK_CONTEXT = 'sp_context'
SPARK_SESSION = 'sp_session'
SQL_CONTEXT = 'sql_context'
HIVE_CONTEXT = 'hive_context'
HIVE_SESSION = 'hive_session'
SPARK_STREAMING = 'streaming'

TYPE_CHOICES = (SPARK_CONTEXT, SPARK_SESSION, SQL_CONTEXT, HIVE_CONTEXT, HIVE_SESSION, SPARK_STREAMING)


class __tags(object):
    def __init__(self):
        self.sql = {'sql'}
        self.hive = {'hive', 'sql'}
        self.streaming = {'streaming'}


tags = __tags()
