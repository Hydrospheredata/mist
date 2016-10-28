######Mist Scala MQTT Publisher

```scala
import io.hydrosphere.mist.lib.{MQTTPublisher, MistJob}

object MyScalaMistJob extends MistJob with MQTTPublisher {

    override def doStuff(parameters: Map[String, Any]): Map[String, Any] = {

      publish("message")

    }
}
```

######Mist Python MQTT Publisher 

```python
from mist.mist_job import *

class MyPythonMistJob(MistJob, WithMQTTPublisher):
    def do_stuff(self, parameters):
        self.mqtt.publish("message")

        return "success"
```