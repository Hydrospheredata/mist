# Realtime ML applications
## Overview
Apache Spark is a great engine to train machine learning models at big data scale. After the model is trained there are plenty of use cases when the model could be used outside of Apache Spark. Obviously for single row predictions with low latency and high throughput it does not make sense to hit Apache Spark context. 
On the other hand the line between offline batch predictions, streaming jobs and online low latency queries is very blur. Meaning that the same models could be used in different contexts, and it would be great to use the same machine learning and linear algebra codebase for these use cases. Also for web developers all these internals of batch, streaming and realtime processing should be hidden behind “analytics services” API layer.
With Hydrosphere Mist you are able to train the model in Apache Spark, then the mark this model as “servable” and have it exposed through the REST API.

Apache Spark 2.0 has introduced a concept of ML pipelines and added an ability to serialize fitted model for later use. It is naturally use saved models for single row predictions.
There is work in progress in Apache Spark community to separate MLLib from Apache Spark context and distributed computing. Hydrosphere Mist serving layer is aligned with future MLLib local release and will use it eventually.

Fraud detection, ad serving, preventive maintenance, content recommendations, artificial intelligence (realtime decisioning), NLP, image recognition and other use cases require machine learning model to be embedded into low latency environment of online traffic or events processing systems like AWS Lambda/Kinesis or Apache Flink.

## Tutorial

Let’s take a canonical machine learning tutorial from Apache Spark documentation, extend it to real world scenario and deploy an end-to-end solution with Hydrosphere Mist.

### (1/6) Taking basic Apache Spark ML pipeline
Let's assume you have a model trained in Apache Spark program like [this one](http://spark.apache.org/docs/latest/ml-pipeline.html#example-pipeline).

### (2/6) Mistifying Machine Learning Model

### (3/6) Starting Mist

### (4/6) Deploying training job and serving API

### (5/6) Training the model

### (6/6) Serving/Scoring the model