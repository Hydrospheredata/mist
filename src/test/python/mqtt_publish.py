from mist.mist_job import *

class SimpleSparkContext(MistJob, WithMqttSupport):
    def do_stuff(self, parameters):
        self.mqtt.publish("test python publisher message")
        result = "success"

        return result
