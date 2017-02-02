from mist.mist_job import *

class SimpleSparkContext(MistJob):
    def execute(self, numbers):
        rdd = self.context.parallelize(numbers)
        result = rdd.map(lambda s: 2 * s).collect()

        return {"result": result}
