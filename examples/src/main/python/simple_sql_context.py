from mist.mist_job import *

class SimpleSQLContext(MistJob, WithSQLSupport):

    def do_stuff(self, parameters):
        val = parameters.values()
        patch = val.head()

        df = self.sql_context.read.json(patch)
        df.printSchema()
        df.registerTempTable("people")

        df.show()
        df2 = self.sql_context.sql("SELECT AVG(age) AS avg_age FROM people")
        df2.show()

        result = df2.toJSON().first()

        return result
