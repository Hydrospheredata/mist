from mistpy.decorators import *

@on_streaming_context
def streamingctx_example(ssc):
    log4jLogger = ssc._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger(__name__)
    logger.info("Hello!")

    def takeAndPublish(time, rdd):
        taken = rdd.take(11)
        logger.info(taken)
    
    inputStream = ssc.queueStream(rddQueue)
    mappedStream = inputStream.map(lambda x: (x % 10, 1))
    reducedStream = mappedStream.reduceByKey(lambda a, b: a + b)
    reducedStream.foreachRDD(takeAndPublish)

    ssc.start()
    time.sleep(15)
    ssc.stop()
    return {"result": "ok"}
