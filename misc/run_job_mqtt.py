#!/usr/bin/python

import paho.mqtt.client as mqtt
import json
import uuid

class MqttJobRunner:

    def __init__(self, host, port, topic_in, topic_out):
        self.topic_in = topic_in
        self.topic_out = topic_out

        def on_connect(client, userdata, flags, rc):
            print("Mqtt client connected")
        
        def await_init(data):
            if data["event"] == "initialized" and data["externalId"] == self.externalId:
              self.jobId = data["id"]
              self.status = data["event"]
              print("Job is started with id:" + self.jobId)

        def await_result(data):
            if data["id"] == self.jobId:
                event = data["event"]
                if event == "finished":
                    self.success = True
                    self.finished = True
                    self.result = data["result"]
                elif event == "failed":
                    self.success = False
                    self.finished = True
                    self.error = data
                elif event == "canceled":
                    self.success = False
                    self.finished = True
                    self.result = data
                elif event == "logs":
                    for line in data["events"]:
                        print("Log:" + line["message"])

        def on_message(client, userdata, msg):
            data = json.loads(msg.payload)
            if self.status == "unknown":
                await_init(data)
            else:
                await_result(data)


        mqttc = mqtt.Client()
        mqttc.on_connect = on_connect
        mqttc.on_message = on_message
        mqttc.connect(host, port)
        mqttc.subscribe(self.topic_out, 0)
        self.mqttc = mqttc



    def runJob(self, endpoint, params, runSettings = { "mode": { "type": "shared" }}):
        self.externalId = str(uuid.uuid4())
        req = {
          "endpointId": endpoint,
          "parameters": params,
          "externalId": self.externalId,
          "runSettings": runSettings
        }
        self.mqttc.publish(self.topic_in, json.dumps(req)) 
        self.status = "unknown"
        self.finished = False
        while not self.finished:
            self.mqttc.loop(1)

        # Await logs
        for x in range(0,3):
            self.mqttc.loop(1)

        if self.success:
            return self.result
        else:
            raise Execption("Job "+ endpoint + " jobId:" + self.jobId + "failed. Reason:" + self.result)





runner = MqttJobRunner("localhost", 1883, "in", "out")
runSettings = { "mode": { "type": "shared" }} 
#runSettings = { "mode": { "type": "exclusive", "id": "yoyoyoyo" }} 

result = runner.runJob("simple-context", { "numbers": [1,2,3,4,5] }, runSettings)
print("Job result is:" + json.dumps(result))
