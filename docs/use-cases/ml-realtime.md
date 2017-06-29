# Real-time ML applications
## Overview
Apache Spark is a great engine to train machine learning models at big data scale. After the model is trained, there are plenty of use cases where the model could be used outside Apache Spark. Obviously, for single row predictions with low latency and high throughput, it does not make sense to hit Apache Spark context. 
On the other hand, the line between offline batch predictions, streaming jobs and online low latency queries is very blur. This implies that the same models could be used in different contexts, and it would be great to use the same machine learning and linear algebra codebase for these use cases. Also, for web developers, all these internals of batch, streaming and realtime processing should be hidden behind “analytics services” API layer.
With Hydrosphere Mist, you are able to train the model in Apache Spark and then expose it through the low latency REST API without making it compatible with PMML or exporting it to other production scoring/serving engine.

![Mist Realtime ML serving (scoring)](http://dv9c7babquml0.cloudfront.net/docs-images/mist-realtime-ml-serving-scoring.png)

Apache Spark 2.0 has introduced a concept of ML pipelines and added the capability to serialize fitted model for later use. It is naturally possible to use saved models for single row predictions.
Work is in progress within the Apache Spark community to separate MLLib from Apache Spark context and distributed computing. Hydrosphere Mist serving layer is aligned with future MLLib local release and will use it eventually.

Fraud detection, ad serving, preventive maintenance, content recommendations, artificial intelligence (realtime decisioning), NLP, image recognition and other use cases require machine learning model to be embedded into low latency environment of online traffic or events processing systems like AWS Lambda/Kinesis or Apache Flink.

## Tutorial

**Please note we have moved realtime serving feature to [spark-ml-serving](https://github.com/Hydrospheredata/spark-ml-serving) library and [hydro-serving](https://github.com/Hydrospheredata/hydro-serving) project. So, the tutorial content might be outdated**

Let’s take a canonical machine learning tutorial from Apache Spark documentation, extend it to real world scenario and deploy an end-to-end solution with Hydrosphere Mist.

### (1/6) Taking basic Apache Spark ML pipeline
Let's assume you have a model trained in Apache Spark program like [this one](http://spark.apache.org/docs/latest/ml-pipeline.html#example-pipeline).

### (2/6) Mistifying Machine Learning Model

Assuming you already have machine learning model trained and saved by original Apache Spark program from the basic tutorial. Let's add a model serving API for that:

````
import io.hydrosphere.mist.lib.spark2._
import io.hydrosphere.mist.lib.spark2.ml._

object MLClassification extends MLMistJob with SQLSupport {
  
  override def serve(text: List[String]): Map[String, Any] = {

    import LocalPipelineModel._
    
    val pipeline = PipelineLoader.load(s"regression") // load the same model you've traing in original Apache Spark program
    // Wrap input data set into local data structure
    val data = LocalData(
      LocalDataColumn("text", text)
    )
    // Use the same Pipeline.transform() method for prediction
    val result: LocalData = pipeline.transform(data)
    Map("result" -> result.toMapList)
  }
}
````
A full source code could be found at [https://github.com/Hydrospheredata/mist/blob/master/examples-spark2/src/main/scala/MLClassification.scala](https://github.com/Hydrospheredata/mist/blob/master/examples-spark2/src/main/scala/MLClassification.scala)

Obviously the code above could be generalised to serve any model from the model repository. So, you'll not need to write a single line of code to make the model "servable". We are going to add this abstract entry point in future releases. 

### (3/6) Starting Mist

Start Mist in docker container and mount local directories for your jobs and configs

```
#create jobs and configs directories and mount it to Mist. So, you'll be able to copy new jobs there and edit configs on the fly
mkdir configs
curl -o ./configs/mist.conf https://raw.githubusercontent.com/Hydrospheredata/mist/master/configs/docker.conf
mkdir jobs
docker run -p 2003:2003 --name mist -v /var/run/docker.sock:/var/run/docker.sock -v $PWD/configs:/usr/share/mist/configs -v $PWD/jobs:/jobs -d hydrosphere/mist:master-2.1.0 mist
```

### (4/6) Deploying training job and serving API
Build `.jar` and copy it into `./jobs` directory

```
sbt clean package
cp ./target/scala-2.11/ml-realtime.jar ./jobs
```

Create or edit file `./configs/router.conf` to add an ml model training and serving router:

````
classification = {
    path = '/jobs/ml-realtime.jar', // local or HDFS file path
    className = MLClassification$',
    namespace = 'production-namespace'
}
````


### (5/6) Training the model
For the demo purposes it is convenient to train the model right from the Mist web console since it is also axposed as a REST API.

Go to `http://localhost:2003/ui/#`, select `classification` route and send `train` action.

![Mist UI train the model](http://dv9c7babquml0.cloudfront.net/docs-images/mist-ui-train-ml-model.png)

### (6/6) Serving/Scoring the model

Mist web console could be used to try serving action as well.

Let's use `curl` instead to measure latency.

```
$ time curl --header "Content-Type: application/json" -X POST http://localhost:2003/api/classification?serve --data """{\"text\": [\"mist makes spark better\"] }"""
{"success":true,"payload":{"result":[{"features":{"size":1000,"indices":[105,691,941,991],"values":[1.0,1.0,1.0,1.0]},"text":"mist makes spark better","prediction":0.0,"words":["mist","makes","spark","better"],"rawPrediction":{"values":[0.16293291377588215,-0.16293291377588215]},"probability":{"values":[0.5406433544852302,0.45935664551476996]}}]},"errors":[],"request":{"path":"/jobs/mist_examples_2.11-0.10.0.jar","className":"MLClassification$","namespace":"regression","parameters":{"text":["mist makes spark better"]},"route":"classification"}}
real	0m0.056s
user	0m0.003s
sys	0m0.006s
```
