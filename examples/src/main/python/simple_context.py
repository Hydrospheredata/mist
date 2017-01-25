# coding=utf-8

from mist.mist_job import MistJob

class SimpleContext(MistJob):

    def do_stuff(self, params):
        print(str(params))
        print(type(params))
        val = params.values()
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

        return result
