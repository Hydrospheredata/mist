##Configuration

Mist creates and orchestrates Apache Spark contexts automatically. All created contexts have their own name. Every job is run in a namespace. By default when you request a job to run the first time, Mist creates a namespace and new Spark context. The second request will use the created namespace so the context will be alive while Mist is running. This behavior can be changed in the configuration file: `mist.contextSetting.onstart` allows you to specify namespaces which must be run on start; `disposable` setting kills the context right after using it. You can set up options either for all contexts (`mist.contextDefault`) or for individual contexts (`mist.context.<namespace>`).


Configuration files are in [HOCON format](https://github.com/typesafehub/config/blob/master/HOCON.md)
```hocon
# spark master url can be either of three: local, yarn, mesos (local by default)
mist.spark.master = "local[*]"

# http interface (off by default)
mist.http.on = false
mist.http.host = "0.0.0.0"
mist.http.port = 2003

# MQTT interface (off by default)
mist.mqtt.on = false
mist.mqtt.host = "192.168.10.33"
mist.mqtt.port = 1883
# mist listens this topic for incoming requests
mist.mqtt.subscribe-topic = "foo"
# mist answers in this topic with the results
mist.mqtt.publish-topic = "foo"

# recovery job (off by default)
mist.recovery.on = false
# mist.recovery.multilimit = 10
# mist.recovery.typedb = "MapDb"
# mist.recovery.dbfilename = "file.db"

# default settings for all contexts
# timeout for each job in context
mist.context-defaults.timeout = 100 days
# mist can kill context after job finished (off by default)
mist.context-defaults.disposable = false

# settings for SparkConf
mist.context-defaults.spark-conf = {
    spark.default.parallelism = 128
    spark.driver.memory = "10g"
    spark.scheduler.mode = "FAIR"
}

# settings can be overridden for each context
mist.contexts.foo.timeout = 100 days

mist.contexts.foo.spark-conf = {
    spark.scheduler.mode = "FIFO"
}

mist.contexts.bar.timeout = 1000 second
mist.contexts.bar.disposable = true

# mist can create context on start, so we don't waste time on first request
mist.context-settings.onstart = ["foo"]

# mist log level
mist.akka {
  # Event handlers to register at boot time (Logging$DefaultLogger logs to STDOUT)
  # loggers = ["akka.event.Logging$DefaultLogger"]
  loggers = ["akka.event.slf4j.Slf4jLogger"]

  event-handlers = ["akka.event.slf4j.Slf4jEventHandler"]

  # Log level used by the configured loggers (see "event-handlers") as soon
  # as they have been started; before that, see "stdout-loglevel"
  # Options: OFF, ERROR, WARNING, INFO, DEBUG
  loglevel = "INFO"

  # logging-filter = "akka.event.slf4j.Slf4jLoggingFilter"

  # Log level for the very basic logger activated during AkkaApplication startup
  # Options: OFF, ERROR, WARNING, INFO, DEBUG
  stdout-loglevel = "INFO"

  log-config-on-start = on
}
```
