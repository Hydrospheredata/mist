from mist.mist_job import *

class SimpleStreaming(MistJob, WithStreamingContext, WithMqttPublisher):

    def execute(self, parameters):
        import time

        def take_and_publish(time, rdd):
            taken = rdd.take(11)
            self.mqtt.publish("-------------------------------------------")
            self.mqtt.publish("Time: %s" % time)
            self.mqtt.publish("-------------------------------------------")
            self.mqtt.publish(str(taken))

        ssc = self.streaming_context
        type(ssc)
        rdd_queue = []
        for _ in range(500):
            rdd_queue += [ssc.sparkContext.parallelize([j for j in range(1, 1001)], 10)]

        # Create the QueueInputDStream and use it do some processing
        input_stream = ssc.queueStream(rdd_queue)
        mapped_stream = input_stream.map(lambda x: (x % 10, 1))
        reduced_stream = mapped_stream.reduceByKey(lambda a, b: a + b)
        #reduced_stream.pprint()

        reduced_stream.foreachRDD(take_and_publish)

        ssc.start()
        time.sleep(15)
        ssc.stop(stopSparkContext=False, stopGraceFully=False)

        result = "success"

        return {"result": result}
