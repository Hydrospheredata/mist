from mist.mist_job import *

class SimpleStreaming(MistJob, WithStreamingContext, WithPublisher):

    def execute(self, parameters):
        import time

        def takeAndPublish(time, rdd):
            taken = rdd.take(11)
            self.publisher.publish("-------------------------------------------")
            self.publisher.publish("Time: %s" % time)
            self.publisher.publish("-------------------------------------------")
            self.publisher.publish(str(taken))

        ssc = self.streaming_context
        type(ssc)
        rddQueue = []
        for i in range(500):
            rddQueue += [ssc.sparkContext.parallelize([j for j in range(1, 1001)], 10)]

        # Create the QueueInputDStream and use it do some processing
        inputStream = ssc.queueStream(rddQueue)
        mappedStream = inputStream.map(lambda x: (x % 10, 1))
        reducedStream = mappedStream.reduceByKey(lambda a, b: a + b)
        #reducedStream.pprint()

        reducedStream.foreachRDD(takeAndPublish)

        ssc.start()
        time.sleep(15)
        ssc.stop(stopSparkContext=False, stopGraceFully=False)

        result = "success"

        return {"result": result}
