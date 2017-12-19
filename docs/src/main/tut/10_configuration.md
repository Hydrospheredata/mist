---
layout: docs
title: "Configuration"
position: 10
---
## Configuration

_See full configuration [here](https://github.com/Hydrospheredata/mist/blob/master/mist/master/src/main/resources/master.conf)_

Configuration files are in [HOCON format](https://github.com/typesafehub/config/blob/master/HOCON.md)

### Enable async interfaces to receive events

Kafka:

| Property name                         | Default value         | Meaning                                                                                                                                                                                                                                                                                                                                                                                |
|---------------------------------------|-----------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `mist.kafka.on`                        | false                 | Turning on/off support of async jobs with [MQTT](http://mqtt.org/)                                                                                                                                                                                                                                                                                                                     |
| `mist.kafka.host`                      | localhost             | Broker host                                                                                                                                                                                                                                                                                                                                                                       |
| `mist.kafka.port`                      | 9092                  | Broker port                                                                                                                                                                                                                                                                                                                                                                       |
| `mist.kafka.subscribe-topic`           | _no default value_    | Topic Mist listens to for incoming requests                                                                                                                                                                                                                                                                                                                                          |
| `mist.kafka.publish-topic`             | _no default value_    | Topic Mist writes response into                                                                                                                                                                                                                                                                                                                                          |

MQTT:

| Property name                         | Default value         | Meaning                                                                                                                                                                                                                                                                                                                                                                                |
|---------------------------------------|-----------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `mist.mqtt.on`                        | false                 | Turning on/off support of async jobs with [MQTT](http://mqtt.org/)                                                                                                                                                                                                                                                                                                                     |
| `mist.mqtt.host`                      | localhost             | MQTT server host                                                                                                                                                                                                                                                                                                                                                                       |
| `mist.mqtt.port`                      | 1883                  | MQTT server port                                                                                                                                                                                                                                                                                                                                                                       |
| `mist.mqtt.subscribe-topic`           | _no default value_    | Topic Mist listens to for incoming requests                                                                                                                                                                                                                                                                                                                                          |
| `mist.mqtt.publish-topic`             | _no default value_    | Topic Mist writes response into                                                                                                                                                                                                                                                                                                                                          |

### Enable Kerberos

Example:

```hocon
mist {
  security {
    enabled = true
    keytab = ${path_to_keytab}
    principal = ${your_principal}
    interval = 30 minutes # default 1 hour
  }
}
```
