from mist.mist_job import *

class SessionJob(MistJob, WithHiveSupport):
    def execute(self, path):
        df = self.session.read.json(path)
        df.printSchema()
        df.registerTempTable("people")

        df.show()
        df2 = self.session.sql("SELECT AVG(age) AS avg_age FROM people")
        df2.show()

        result = df2.toJSON().first()

        return {"result": result}
