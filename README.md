[![Build Status](https://travis-ci.org/Hydrospheredata/mist.svg)](https://travis-ci.org/Hydrospheredata)
# Mist


Mist – a thin service on top of Spark which makes it possible to execute Scala & Python Spark Jobs from application layers and get synchronous, asynchronous, and reactive results as well as provide an API to external clients.

## Code Example

For start Spark Jobs you need deploy and start Mist app, override definition doStuff and send HTTP or MQTT request

Scala example (Mist/examples/SimpleContext.scala)

Python example (Mist/examples/example.py)

## Motivation



## Installation

 Clone latest version mist from github
```
git clone https://github.com/provectus/mist
```
`vagrant up`
`ssh vagrant`
`cd /vagrant`
`./sbt/sbt run`



## API Reference



## Tests

Before send you request make settings you reference.conf (YouMistHost, YouMqttPort, YouHttpPort) and start Mist app

####MQTT

######Scala
mosquitto_pub -h YouMistHost -p YouMqttPort -m '{"jarPath":"/PatchToMist/examples/target/scala-2.11/mist_examples_2.11-0.0.1.jar", "className":"SimpleContext$","parameters":{"digits":[1,2,3,4,5,6,7,8,9,0]}, "external_id":"12345678","name":"foo"}'  -t 'foo'

######Python
mosquitto_pub -h YouMistHost -p YouMqttPort -m '{"pyPath":"/PatchToMist/examples/src/main/python/example.py","parameters":{"digits":[1,2,3,4,5,6,7,8,9,0]}, "external_id":"12345678","name":"foo"}'  -t 'foo'

####HTTP
######Scala
curl --header "Content-Type: application/json" -X POST http://YouMistHost:YouHttpPort/jobs --data '{"jarPath":"/PatchToMist/examples/target/scala-2.11/mist_examples_2.11-0.0.1.jar", "className":"SimpleContext$","parameters":{"digits":[1,2,3,4,5,6,7,8,9,0]}, "external_id":"12345678","name":"foo"}'

curl --header "Content-Type: application/json" -X POST http://YouMistHost:YouHttpPort/jobs --data '{"jarPath":"/PatchToMist/examples/target/scala-2.11/mist_examples_2.11-0.0.1.jar","className":"SimpleSQLContext$","parameters":{"file":"/PatchToMist/examples/resources/SimpleSQLContextData.json"}, "external_id":"12345678","name":"foo"}'

######Python
curl --header "Content-Type: application/json" -X POST http://YouMistHost:YouHttpPort/jobs --data '{"pyPath":"/PatchToMist/examples/src/main/python/example.py", "parameters":{"digits":[1,2,3,4,5,6,7,8,9,0]}, "external_id":"12345678","name":"foo"}'

curl --header "Content-Type: application/json" -X POST http://YouMistHost:YouHttpPort/jobs --data '{"pyPath":"/PatchToMist/examples/src/main/python/exampleSQL.py", "parameters":{"file":"/PatchToMist/examples/resources/SimpleSQLContextData.json"}, "external_id":"12345678","name":"foo"}'

AllTests
>./sbt/sbt test

## Contributors

>http://hydrosphere.io/
>https://www.linkedin.com/company/hydrospherebigdata
>https://www.facebook.com/hydrosphere.io/
>https://twitter.com/hydrospheredata

## License


