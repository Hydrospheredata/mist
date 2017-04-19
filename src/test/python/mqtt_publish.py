from mist.mist_job import *

class SimpleSparkContext(MistJob, WithMqttPublisher):
    def execute(self):
        self.mqtt.publish("test python publisher message")
        result = "success"

        return {"result": result}
