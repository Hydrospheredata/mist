## Local ML models

Mist experimentally supports local serving of your ML pipelines.

### Supported models

Preprocessing:
  - [Binarizer](./../examples-spark2/src/main/scala/BinarizerJob.scala)
  - [CountVectorizer](./../examples-spark2/src/main/scala/CountVectorizerJob.scala)
  - [StandardScaler](./../examples-spark2/src/main/scala/StandardScalerJob.scala)
  - [StopWordsRemover](./../examples-spark2/src/main/scala/StopWordsRemoverJob.scala)
  - [StringIndexer](./../examples-spark2/src/main/scala/StringIndexerJob.scala)
  - [Tokenizer](./../examples-spark2/src/main/scala/MLClassification.scala)
  - [HashingTF](./../examples-spark2/src/main/scala/MLClassification.scala)
  - [IndexToString](./../examples-spark2/src/main/scala/IndexToStringJob.scala)
  - [MaxAbsScaler](./../examples-spark2/src/main/scala/MaxAbsScalerJob.scala)
  - [MinMaxScaler](./../examples-spark2/src/main/scala/MinMaxScalerJob.scala)
  - [NGram](./../examples-spark2/src/main/scala/NgramJob.scala)
  - [OneHotEncoder](./../examples-spark2/src/main/scala/OneHotEncoderJob.scala)
  - [PCA](./../examples-spark2/src/main/scala/PCAJob.scala)
  - [Normalizer](./../examples-spark2/src/main/scala/NormalizerJob.scala)
  - [VectorIndexer](./../examples-spark2/src/main/scala/DTreeRegressionJob.scala)
  - [PolynomialExpansion](./../examples-spark2/src/main/scala/PolynomialExpansionJob.scala)
  - [Discrete Cosine Transform](./../examples-spark2/src/main/scala/DCTJob.scala)
  - [Word2Vec](./../examples-spark2/src/main/scala/Word2VecJob.scala)
  
Classification:
  - [DecisionTreeClassification](./../examples-spark2/src/main/scala/DTreeClassificationJob.scala)
  - [RandomForestClassification](./../examples-spark2/src/main/scala/RandomForestClassificationJob.scala)
  - [LogisticRegression](./../examples-spark2/src/main/scala/MLClassification.scala)
  - [NaiveBayes](./../examples-spark2/src/main/scala/NaiveBayesJob.scala)

Regression:
  - [LinearRegression](./../examples-spark2/src/main/scala/LinearRegressionJob.scala)
  - [DecisionTreeRegression](./../examples-spark2/src/main/scala/DTreeRegressionJob.scala)

Clusterisation:
  - [KMeans](./../examples-spark2/src/main/scala/KMeansJob.scala)
