# coding=utf-8
import time

from mist.mist_job import MistJob

class SimpleContext(MistJob):

    def execute(self, numbers, multiplier = 2):
        rdd = self.context.parallelize(numbers)
        result = rdd.map(lambda s: s * multiplier).collect()
        time.sleep(60)
        return {"result": result}
