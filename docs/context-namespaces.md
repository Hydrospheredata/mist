## Context namespaces

Mist creates and orchestrates Apache Spark contexts automatically.
All created contexts have their own namespace. Every job is run in a namespace.
In fact namespace describes a named Spark context and Mist settings for this Spark context.
For reasons of simplicity namespace is an arbitrary string which is specified by user e.g. `foo`, `report`, `forecast_only_namespace`.

By default when you request a job to run the first time, Mist creates a namespace and new Spark context.
The second request will use the created namespace so the context will be alive while Mist is running.
This behavior can be changed using context configuration:
```hocon
mist {
  context {
   foo {
     precreated = true
   }
```
`precreated` key allows you to specify namespaces which must be run on start

You can set up options either for all contexts (`mist.context-defaults`) or for each context individually (`mist.context.<namespace>`)
including [spark settings](http://spark.apache.org/docs/latest/configuration.html) (`mist.context.<namespace>.spark-conf`)
