from mist.mist_job import *

class SimpleSparkContext(MistJob, WithMQTTPublisher):
    def execute(self, numbers):
        self.mqtt.publish("test python publisher message")
        rdd = self.context.parallelize(numbers)
        result = rdd.map(lambda s: 2 * s).collect()
        self.mqtt.publish(str(result).strip('[]'))

        return {"result": result}

