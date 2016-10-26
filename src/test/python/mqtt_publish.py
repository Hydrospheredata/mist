from mist.mist_job import *

class SimpleSparkContext(MistJob):
    def do_stuff(self, parameters):
        val = parameters.values()
        list = val.head()
        size = list.size()
        pylist = []
        count = 0
        while count < size:
            pylist.append(list.head())
            count = count + 1
            list = list.tail()

        rdd = self.context.parallelize(pylist)
        result = rdd.map(lambda s: 2 * s).collect()
        self.publisher.pub_mqtt(str(result).strip('[]'))

        return result
