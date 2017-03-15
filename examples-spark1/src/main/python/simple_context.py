# coding=utf-8

from mist.mist_job import MistJob

class SimpleContext(MistJob):

    def execute(self, numbers, multiplier = 2):
        rdd = self.context.parallelize(numbers)
        result = rdd.map(lambda s: s * multiplier).collect()

        return {"result": result}
