import mist

class MyJob:
    def __init__(self, job):
        job.sendResult(self.doStuff(job))

    def doStuff(self, job):
        val = job.parameters.values()
        list = val.head()
        size = list.size()
        pylist = []
        count = 0
        while count < size:
            pylist.append(list.head())
            count = count + 1
            list = list.tail()


        rdd = job.sc.parallelize(pylist)
        result = rdd.map(lambda s: 2 * s).collect()

        return result

job = MyJob(mist.Job())