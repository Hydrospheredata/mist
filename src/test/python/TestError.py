import mist

class MyJob:
    def __init__(self, job):
        job.sendResult(self.doStuff(job))

    def doStuff(self, job):

        err = job._entry_point.errorWrapper()
        err.set(sys.argv[2], "TestError")
        result = 1/0

        return result

job = MyJob(mist.Job())
