from mist.mist_job import *

class SimpleSparkContext(MistJob):
    def do_stuff(self, parameters):
        self.publisher.pub_mqtt("test python publisher message")
        result = "success"

        return result
