---
layout: docs
title: "Configuration"
permalink: configuration.html
position: 10
---
## Configuration

_See full configuration [here](https://github.com/Hydrospheredata/mist/blob/master/mist/master/src/main/resources/master.conf)_

Configuration files are in [HOCON format](https://github.com/typesafehub/config/blob/master/HOCON.md)

### Network configuration

| Property name                         | Default value         | Meaning                     |
|---------------------------------------|-----------------------|-----------------------------|
|`mist.cluster.host`                    | 0.0.0.0               | default communication host  |
|`mist.cluster.port`                    | 2551                  | default communication port  |
|`mist.http.host`                       | 0.0.0.0               | default http host           |
|`mist.http.port`                       | 2004                  | default http port           |
|`mist.log-service.host`                | 0.0.0.0               | default http host           |
|`mist.log-service.port`                | 2005                  | default http port           |

Also it's possible to use `auto` value for host configuration keys - mist automactically infers the best matched host address.
```
mist.cluster.host = "auto"
```
It's useful in case when mist works in environment where we need to set up public visible address.
For example: running mist-worker in separate docker container, using kubernetes cluster for spark

### Database configuration

To store information about jobs and their statuses Mist uses H2 as database.
From v1.1.2 it's also possible to use PostgreSQL.
Example:
```
mist.db {
  poolSize = 32
  driverClass = "org.postgresql.Driver"
  jdbcUrl = "jdbc:postgresql:mist"
  username = "..."
  password = "..."
}
```
By default, automatic db migration is enabled. It means that the provided user should be allowed to create tables.
In case if it's not possible you can disable it and maintain your database manually using [these migration scripts](https://github.com/Hydrospheredata/mist/tree/master/mist/master/src/main/resources/db/migrations/postgresql).
```
mist.db {
  migration = off
}
```


### Worker configuration

| Property name                         | Default value         | Meaning                           |
|---------------------------------------|-----------------------|-----------------------------------|
|`mist.workers.runner-init-timeout`          | 2 minutes             | Worker creation timeout           |
|`mist.workers.ready-timeout`                | 2 minutes             | Spark context creation timeout    |
|`mist.workers.runner`                       | local                 | How mist spawn workers            |

#### Runners

There are several ways how mist spawn workers:
- `local` - just local `submit-submit` on the same node
- `docker` - Spawn worker in separated docker container
- `manual` - custom script for worker spawning - [example](https://github.com/Hydrospheredata/mist/blob/master/examples/misc/manual_worker_start.py).

See additional configuration for `docker` and `manual` runners bellow:

##### Docker

| Property name                                  | Default value                             | Meaning                               
|------------------------------------------------|-------------------------------------------|---------------------------------------
|`workers.docker.host`                           | unix:///var/run/docker.sock               | Docker server adress                  
|`workers.docker.image`                          | hydrosphere/mist:{{ site.version }}-2.3.0"| Mist docker image                     
|`workers.docker.mist-home`                      | /usr/share/mist                           | Path to mist home inside image        
|`workers.docker.spark-home`                     | /usr/share/spark                          | Path to spark home inside image       
|`workers.docker.network-type`                   | net                                       | Docker network driver. Allowed values: `net`, `bridge`, `auto-master` - clone master network settings if it's runned inside container
|`worker.docker.auto-master-network.container.id`|                                           | Mist master docker container id ([usage](https://github.com/Hydrospheredata/mist/blob/master/docker-entrypoint.sh#L7)) 

##### Manual 

| Property name                                  | Default value | Meaning             |
|------------------------------------------------|---------------|---------------------|
|`workers.manual.startCmd`                       |               |path to start script |
|`workers.manual.stopCmd`                        |               |path to stop script  |


### Enable async interfaces to receive events

Kafka:

| Property name                         | Default value         | Meaning                                                                                                                                                                                                                                                                                                                                                                                |
|---------------------------------------|-----------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `mist.kafka.on`                        | false                 | Turning on/off support of async jobs with [kafka](https://kafka.apache.org/)                                                                                                                                                                                                                                                                                                                     |
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

### Default context

To override settings for `default` context use prefix `mist.context-defaults` - (master.conf)[https://github.com/Hydrospheredata/mist/blob/master/mist/master/src/main/resources/master.conf#L73].


