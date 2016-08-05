import mist_sparksession

class MyJob:
    def __init__(self, job):
        job.sendResult(self.doStuff(job))

    def doStuff(self, job):
        val = job.parameters.values()
        patch = val.head()

        df = job.ss.read.json(patch)
        df.printSchema()
        df.registerTempTable("people")

        df.show()
        df2 = job.ss.sql("SELECT AVG(age) AS avg_age FROM people")
        df2.show()

        result = df2.toJSON().first()

        return result

job = MyJob(mist_sparksession.Job())