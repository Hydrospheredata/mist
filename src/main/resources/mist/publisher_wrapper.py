class PublisherWrapper:

    _mqtt = None

    def __init__(self):
        pass

    def set_mqtt(self, java_gateway):
        self._mqtt = java_gateway.entry_point.mqttPublisher()

    @property
    def mqtt(self):
        return self._mqtt
