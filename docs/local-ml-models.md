## Local ML models

Mist experimentally supports local serving of your ML pipelines.

### Supported models

Preprocessing:
  - [Binarizer](./../examples/src/main/scala/BinarizerJob_Spark2.scala)
  - [CountVectorizerModel](./../examples/src/main/scala/CountVectorizerJob_Spark2.scala)
  - [StandardScaler](./../examples/src/main/scala/StandardScalerJob_Spark2.scala)
  - [StopWordsRemover](./../examples/src/main/scala/StopWordsRemoverJob_Spark2.scala)
  - [StringIndexer](./../examples/src/main/scala/StringIndexerJob_Spark2.scala)
  - [Tokenizer](./../examples/src/main/scala/MLClassification_Spark2.scala)
  - [HashingTF](./../examples/src/main/scala/MLClassification_Spark2.scala)
  - [IndexToString](./../examples/src/main/scala/IndexToStringJob_Spark2.scala)
  - [MaxAbsScaler](./../examples/src/main/scala/MaxAbsScalerJob_Spark2.scala)
  - [MinMaxScaler](./../examples/src/main/scala/MinMaxScalerJob_Spark2.scala)
  - [NGram](./../examples/src/main/scala/NgramJob_Spark2.scala)
  - [OneHotEncoder](./../examples/src/main/scala/OneHotEncoderJob_Spark2.scala)
  - [PCA](./../examples/src/main/scala/PCAJob_Spark2.scala)
  - [Normalizer](./../examples/src/main/scala/NormalizerJob_Spark2.scala)
  - [VectorIndexer](./../examples/src/main/scala/DTreeRegressionJob_Spark2.scala)
  - [PolynomialExpansion](./../examples/src/main/scala/PolynomialExpansionJob_Spark2.scala)
  - [Discrete Cosine Transform](./../examples/src/main/scala/DCTJob_Spark2.scala)
  - [Word2VecModel](./../examples/src/main/scala/Word2VecJob_Spark2.scala)

Classification:
  - [DecisionTreeClassification](./../examples/src/main/scala/DTreeClassificationJob_Spark2.scala)
  - [RandomForestClassification](./../examples/src/main/scala/RandomForestClassificationJob_Spark2.scala)
  - [LogisticRegression](./../examples/src/main/scala/MLClassification_Spark2.scala)

Regression:
  - [LinearRegression](./../examples/src/main/scala/LinearRegressionJob_Spark2.scala)
  - [DecisionTreeRegression](./../examples/src/main/scala/DTreeRegressionJob_Spark2.scala)

Clusterisation:
  - [KMeansModel](./../examples/src/main/scala/KMeansJob_Spark2.scala)