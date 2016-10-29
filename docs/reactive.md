Sometimes during working of Mist Job need to receive intermediate results or messages, for example in the case of a data streaming.

For this purpose in Mist Job method **publish** was implemented.

Currently, there exists a realization for publication in MQTT, for it is enough to add to your Job **MQTTPublisher** and sending messages to the topic MQTT will have carried out by the **publish** before completion of Job.

######Mist Scala MQTT Publisher

```scala
import io.hydrosphere.mist.lib.{MQTTPublisher, MistJob}

object MyScalaMistJob extends MistJob with MQTTPublisher {

    override def doStuff(parameters: Map[String, Any]): Map[String, Any] = {
      ...
      publish("message")
      ...
      Map("result" -> "success")
    }
}
```

######Mist Python MQTT Publisher 

```python
from mist.mist_job import *

class MyPythonMistJob(MistJob, WithMQTTPublisher):
    def do_stuff(self, parameters):
    	...
        self.mqtt.publish("message")
        ...
        return "success"
```