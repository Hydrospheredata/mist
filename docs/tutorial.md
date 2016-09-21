## Tutorial

Here we show how to get Mist running on localhost.  You will obviously need Spark installed.  In this example we use MQTT, but you could use HTTP instead by selecting that option in the Mist configuration file.

Install Moquito:

	sudo apt-get install mosquitto mosquitto-clients

Build Mist as explained here[https://github.com/Hydrospheredata/mist#getting-started-with-mist].

Create this configuration file mist.conf and save it someplace.  In this example we use /usr/src/mist/mist/mist.conf.


```hocon
mist.spark.master = "local[*]"

mist.settings.thread-number = 16

mist.http.on = true
mist.http.host = "127.0.0.1"
mist.http.port = 2003

mist.mqtt.on = false

mist.recovery.on = false

mist.contexts.foo.timeout = 100 days

mist.contexts.foo.spark-conf = {
  spark.default.parallelism = 4
  spark.driver.memory = "128m"
  spark.executor.memory = "64m"
  spark.scheduler.mode = "FAIR"
}

```

Next start Mist like this, changing the mist-assembly-X.X.X.jar file name to match the version you installed:

         ./mist.sh --config /usr/src/mist/mist/mist.conf --jar /usr/src/mist/mist/mistsrc/mist/target/scala-2.10/mist-assembly-0.4.0.jar
         
Set Python Path as shown below, again adjusting the file names and paths to match your installation:

        export PYTHONPATH=$PYTHONPATH:/usr/src/mist/mist/mistsrc/mist/src/main/python:$SPARK_HOME/python/:$SPARK_HOME/python/lib/py4j-0.9-src.zip

Copy the code from above and save the sample Python code somewhere. The sample program iterates over and prints the parameters sent to it at runtime.
        
Run the sample using curl:

        curl --header "Content-Type: application/json" -X POST http://127.0.0.1:2003/jobs --data '{"pyPath":"/path to your file/Samplecode.py", "parameters":{"digits":[1,2,3,4,5,6,7,8,9,0]}, "external_id":"12345678","name":"foo"}'

If everything is set up correctly it should say something like this message below, plus you will see messages in the Mist stdout console.

        {"success":true,"payload":{"result":[2,4,6,8,10,12,14,16,18,0]},"errors":[],"request":{"pyPath":"/home/walker/Documents/hydrosphere/mistExample.py","name":"foo","parameters":{"digits":[1,2,3,4,5,6,7,8,9,0]},"external_id":"12345678"}}
        
Here is part of Mist stdout log where you can see that the program was submitted to Spark.  The output of the collect() statement will echo there as well.

        16/05/19 12:55:07 INFO SparkContext: Starting job: collect at /home/walker/Documents/hydrosphere/mistExample.py:16
        16/05/19 12:55:07 INFO DAGScheduler: Got job 0 (collect at /home/walker/Documents/hydrosphere/mistExample.py:16) with 4 output partitions
        16/05/19 12:55:07 INFO DAGScheduler: Final stage: ResultStage 0 (collect at /home/walker/Documents/hydrosphere/mistExample.py:16)
        16/05/19 12:55:07 INFO DAGScheduler: Parents of final stage: List()
        16/05/19 12:55:07 INFO DAGScheduler: Missing parents: List()
        16/05/19 12:55:07 INFO DAGScheduler: Submitting ResultStage 0 (PythonRDD[1] at collect at /home/walker/Documents/hydrosphere/mistExample.py:16), which has no missing parents

