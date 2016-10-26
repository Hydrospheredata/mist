import paho.mqtt.client as mqtt

class Publisher:

    _gateway = None

    def set_gateway(self, java_gateway):
        self._gateway = java_gateway

    def pub_mqtt(self, message):
        mqtt_config = self._gateway.entry_point.mqttConfigWrapper()
        client = mqtt.Client()
        client.connect(mqtt_config.getHost(), mqtt_config.getPort(), 60)
        client.publish(mqtt_config.getTopic(), payload=message, qos=0, retain=False)
        client.disconnect()