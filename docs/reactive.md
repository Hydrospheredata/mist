Streaming jobs usualy produce insights to be consumed by applications in realtime. 
It is possible to run Apache Spark streaming job with Mist and subscribe to the insights stream emitted by the job.

Currently MQTT is supported as a transport layer and could be easily extended with Kafka, AMPQ, AWS Kinesis and other messaging systems. 

Method **publish** is a simple and unified abstraction to be used for streaming job results to consumer.

Please note that usually it does not make sense to start streaming job from HTTP request. You could use [Mist CLI] (https://github.com/Hydrospheredata/mist/blob/master/bin/mist) to start particular streaming job. 

###### Reactive Scala job

```scala
import io.hydrosphere.mist.lib.{MqttPublisher, MistJob}

object MyStreamingMistJob extends MistJob with MqttPublisher {

    override def execute(): Map[String, Any] = {
      ...
      reducedStream.foreachRDD{ (rdd, time) =>
         publish("time: " + time)
         publish("lenght: " + rdd.collect().length)
         publish("collection: " + (rdd.collect().toList).toString)
      }
      ...
      Map("result" -> "success")
    }
}
```

###### Reactive Python job 

```python
from mist.mist_job import *

class MyPythonMistJob(MistJob, WithMqttPublisher):
    def execute(self):
    	...
        self.mqtt.publish("message")
        ...
        return "success"
```
