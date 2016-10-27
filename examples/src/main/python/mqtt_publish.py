from mist.mist_job import *

class SimpleSparkContext(MistJob, WithMqttSupport):
    def do_stuff(self, parameters):
        self.mqtt.publish("test python publisher message")
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
        self.mqtt.publish(str(result).strip('[]'))

        return result

