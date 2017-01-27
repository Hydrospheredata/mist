from mist.mist_job import *

class SimpleHiveContext(MistJob, WithSQLSupport, WithHiveSupport):

    def execute(self, path):
        df = self.hive_context.read.json(path)
        df.printSchema()
        df.registerTempTable("people")

        df.show()
        df2 = self.hive_context.sql("SELECT AVG(age) AS avg_age FROM people")
        df2.show()

        result = df2.toJSON().first()

        return {"result": result}
