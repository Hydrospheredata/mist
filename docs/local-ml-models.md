## Local ML models

Mist experimentally supports local serving of your ML pipelines.

### Supported models

Preprocessing:
  - Binarizer
  - Bucketizer
  - DCT
  - ElementwiseProduct
  - HashingTF
  - IndexToString
  - Interaction
  - MaxAbsScaler
  - MinMaxScaler
  - NGram
  - Normalizer
  - OneHotEncoder
  - PCA
  - PolynomialExpansion
  - SQLTransformer
  - StandardScaler
  - StopWordsRemover
  - StringIndexer
  - Tokenizer
  - VectorAssembler
  - VectorIndexer
  - VectorSlicer

Classification:
  - DecisionTreeClassification
  - MultilayerPerceptronClassification
  - RandomForestClassification

Clustering:
  - BisectingKMeans
  - GaussianMixture

Regression:
  - LogisticRegression
  - DecisionTreeRegression
