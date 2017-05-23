package io.hydrosphere.mist.api.ml

import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.regression._
import org.apache.spark.ml.clustering._
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.linalg.{DenseVector => OldDenseVector}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector}
import org.apache.spark.sql.SparkSession
import org.scalatest.{Assertion, BeforeAndAfterAll, FunSpec}
import LocalPipelineModel._


class LocalModelSpec extends FunSpec with BeforeAndAfterAll {
  var session: SparkSession = _

  def modelPath(modelName: String): String = s"./mist-lib/target/trained-models-for-test/$modelName"

  def createInputData[T](name: String, data: List[T]): LocalData = LocalData(LocalDataColumn(name, data))

  def compareDoubles(a: Double, b: Double, threshold: Double = 0.0001): Assertion = {
    assert((a - b).abs < threshold)
  }

  def compareArrDoubles(a: Array[Double], b: Array[Double], threshold: Double = 0.0001): Unit = {
    a.zip(b).foreach{
      case (aa, bb) => compareDoubles(aa, bb, threshold)
    }
  }

  describe("CountVectorizer") {
    val path = modelPath("countvectorizer")

    it("should train") {
      import org.apache.spark.ml.feature.CountVectorizer

      val df = session.createDataFrame(Seq(
        (0, Array("a", "b", "c")),
        (1, Array("a", "b", "b", "c", "a"))
      )).toDF("id", "words")

      val cv = new CountVectorizer()
        .setInputCol("words")
        .setOutputCol("features")
        .setVocabSize(3)
        .setMinDF(2)

      val model = new Pipeline().setStages(Array(cv)).fit(df)
      model.write.overwrite().save(path)
    }

    it("should load") {
      PipelineLoader.load(path)
    }

    it("should transform") {
      val trainedModel = PipelineLoader.load(path)
      val data = LocalData(LocalDataColumn("words", List(List("a", "b", "c"))))
      val result = trainedModel.transform(data).column("features").get.data.map { f =>
        f.asInstanceOf[SparseVector].toArray
      }
      val validation = List(List(1.0, 1.0, 1.0))

      result zip validation foreach {
        case (arr: Array[Double], validRow: List[Double]) => assert(arr === validRow)
      }
    }
  }

  describe("NGram") {
    val path = modelPath("ngram")

    it("should train") {
      val df = session.createDataFrame(Seq(
        (0, Array("Provectus", "is", "such", "a", "cool", "company")),
        (1, Array("Big", "data", "rules", "the", "world")),
        (2, Array("Cloud", "solutions", "are", "our", "future"))
      )).toDF("id", "words")

      val ngram = new NGram().setN(2).setInputCol("words").setOutputCol("ngrams")
      val pipeline = new Pipeline().setStages(Array(ngram))
      val model = pipeline.fit(df)

      model.write.overwrite().save(path)
    }

    it("should load") {
      val model = PipelineLoader.load(path)
    }

    it("should transform") {
      val trainedModel = PipelineLoader.load(path)
      val data = LocalData(LocalDataColumn("words", List(List("Provectus", "team", "is", "awesome"))))
      val result = trainedModel.transform(data).column("ngrams").get.data
      val validation = Array(List("Provectus team", "team is", "is awesome"))

      result zip validation foreach {
        case (arr: List[String], validRow: List[String]) => assert(arr === validRow)
      }
    }
  }

  describe("StandardScaler") {
    val path = modelPath("standardscaler")

    it("should train") {
      val data = Array(
        Vectors.dense(0.0, 10.3, 1.0, 4.0, 5.0),
        Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
        Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0)
      )
      val df = session.createDataFrame(data.map(Tuple1.apply)).toDF("features")

      val scaler = new StandardScaler()
        .setInputCol("features")
        .setOutputCol("scaledFeatures")
        .setWithStd(true)
        .setWithMean(false)

      val pipeline = new Pipeline().setStages(Array(scaler))

      val model = pipeline.fit(df)

      model.write.overwrite().save(path)
    }

    it("should load") {
      PipelineLoader.load(path)
    }

    it("should transform") {
      val trainedModel = PipelineLoader.load(path)
      val data = LocalData(LocalDataColumn("features", List(
        List(1.0, 0.0, 1.0, 2.0, 0.0),
        List(2.0, 0.0, 3.0, 4.0, 5.0),
        List(4.0, 0.0, 0.0, 6.0, 7.0)
      )))
      val result = trainedModel.transform(data).column("scaledFeatures").get.data.map { f =>
        f.asInstanceOf[OldDenseVector].toArray
      }
      val validation = List(
        List(0.5, 0.0, 0.6546536707079772, 1.7320508075688774, 0.0),
        List(1.0, 0.0, 1.9639610121239315, 3.464101615137755, 4.330127018922194),
        List(2.0, 0.0, 0.0, 5.196152422706632, 6.062177826491071)
      )

      result zip validation foreach {
        case (arr: Array[Double], validRow: List[Double]) => assert(arr === validRow)
      }
    }
  }

  describe("StopWordsRemover") {
    val path = modelPath("stopwordsremover")

    it("should train") {
      val df = session.createDataFrame(Seq(
        (0, Seq("I", "saw", "the", "red", "balloon")),
        (1, Seq("Mary", "had", "a", "little", "lamb"))
      )).toDF("id", "raw")

      val remover = new StopWordsRemover()
        .setInputCol("raw")
        .setOutputCol("filtered")

      val pipeline = new Pipeline().setStages(Array(remover))

      val model = pipeline.fit(df)

      model.write.overwrite().save(path)
    }

    it("should load") {
      PipelineLoader.load(path)
    }

    it("should transform") {
      val trainedModel = PipelineLoader.load(path)
      val data = LocalData(LocalDataColumn("raw", List(
        List("I", "saw", "the", "red", "balloon"),
        List("Mary", "had", "a", "little", "lamb")
      )))
      val result = trainedModel.transform(data).column("filtered").get.data
      val validation = Array(List("saw", "red", "balloon"), List("Mary", "little", "lamb"))

      result zip validation foreach {
        case (arr: List[String], validRow: List[String]) => assert(arr === validRow)
      }
    }
  }

  describe("MaxAbsScaler") {
    val path = modelPath("maxabsscaler")

    it("should train") {
      val dataFrame = session.createDataFrame(Seq(
        (0, Vectors.dense(1.0, 0.1, -8.0)),
        (1, Vectors.dense(2.0, 1.0, -4.0)),
        (2, Vectors.dense(4.0, 10.0, 8.0))
      )).toDF("id", "features")

      val scaler = new MaxAbsScaler()
        .setInputCol("features")
        .setOutputCol("scaledFeatures")

      val pipeline = new Pipeline().setStages(Array(scaler))

      val model = pipeline.fit(dataFrame)

      model.write.overwrite().save(path)
    }

    it("should load") {
      PipelineLoader.load(path)
    }

    it("should transform") {
      val trainedModel = PipelineLoader.load(path)
      val data = LocalData(LocalDataColumn("features", List(
        List(1.0, 0.0, 1.0),
        List(2.0, 4.0, 5.0),
        List(0.0, 6.0, 7.0)
      )))
      val result = trainedModel.transform(data).column("scaledFeatures").get.data.map { f =>
        f.asInstanceOf[DenseVector].toArray
      }
      val validation = Array(
        List(0.25, 0.0, 0.125),
        List(0.5, 0.4, 0.625),
        List(0.0, 0.6, 0.875)
      )

      result zip validation foreach {
        case (arr: Array[Double], validRow: List[Double]) => assert(arr === validRow)
      }
    }
  }

  describe("MinMaxScaler") {
    val path = modelPath("minmaxscaler")

    it("should train") {
      val dataFrame = session.createDataFrame(Seq(
        (0, Vectors.dense(1.0, 0.1, -1.0)),
        (1, Vectors.dense(2.0, 1.1, 1.0)),
        (2, Vectors.dense(3.0, 10.1, 3.0))
      )).toDF("id", "features")

      val scaler = new MinMaxScaler()
        .setInputCol("features")
        .setOutputCol("scaledFeatures")

      val pipeline = new Pipeline().setStages(Array(scaler))

      val model = pipeline.fit(dataFrame)

      model.write.overwrite().save(path)
    }

    it("should load") {
      PipelineLoader.load(path)
    }

    it("should transform") {
      val trainedModel = PipelineLoader.load(path)
      val data = LocalData(LocalDataColumn("features", List(
        List(1.0, 0.0, -1.0),
        List(2.0, 1.5, 1.0),
        List(3.0, 1.1, 3.0)
      )))
      val result = trainedModel.transform(data).column("scaledFeatures").get.data
      val validation = Array(
        List(0.0, -0.01, 0.0),
        List(0.5, 0.13999999999999999, 0.5),
        List(1.0, 0.1, 1.0)
      )

      result zip validation foreach {
        case (arr: Array[Double], validRow: List[Double]) => assert(arr === validRow)
      }
    }
  }

  describe("StringIndexer -> OneHotEncoder") {
    val path = modelPath("onehotencoder")

    it("should train") {
      val df = session.createDataFrame(Seq(
        (0, "a"), (1, "b"), (2, "c"),
        (3, "a"), (4, "a"), (5, "c")
      )).toDF("id", "category")

      val indexer = new StringIndexer()
        .setInputCol("category")
        .setOutputCol("categoryIndex")
        .fit(df)

      val encoder = new OneHotEncoder()
        .setInputCol("categoryIndex")
        .setOutputCol("categoryVec")

      val pipeline = new Pipeline().setStages(Array(indexer, encoder))

      val model = pipeline.fit(df)

      model.write.overwrite().save(path)
    }

    it("should load local/transform") {
      PipelineLoader.load(path)
    }

    it("should match") {
      val model = PipelineLoader.load(path)
      val localData = createInputData("category", List("a", "b", "c", "a"))
      val validation = List(Array(1.0, 0.0), Array(0.0, 0.0), Array(0.0, 1.0), Array(1.0, 0.0))
      val result = model.transform(localData)
      val computedResult = result.column("categoryVec").get.data.map(_.asInstanceOf[Array[Double]])
      computedResult.zip(validation).foreach{
        case (x, y) => assert(x === y)
      }
    }
  }

  describe("PCA") {
    val path = modelPath("pca")

    it("should train") {
      val data = Array(
        Vectors.sparse(5, Seq((1, 1.0), (3, 7.0))),
        Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
        Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0)
      )
      val df = session.createDataFrame(data.map(Tuple1.apply)).toDF("features")

      val pca = new PCA()
        .setInputCol("features")
        .setOutputCol("pcaFeatures")
        .setK(3)

      val pipeline = new Pipeline().setStages(Array(pca))

      val model = pipeline.fit(df)

      model.write.overwrite().save(path)
    }

    it("should load") {
      PipelineLoader.load(path)
    }

    it("should transform") {
      val trainedModel = PipelineLoader.load(path)
      val data = LocalData(LocalDataColumn("features", List(
        List(2.0, 0.0, 3.0, 4.0, 5.0),
        List(4.0, 0.0, 0.0, 6.0, 7.0)
      )))
      val result = trainedModel.transform(data).column("pcaFeatures").get.data map { f =>
        f.asInstanceOf[OldDenseVector].toArray
      }
      val validation = Array(
        List(-4.645104331781534, -1.1167972663619026, -5.524543751369387),
        List(-6.428880535676489, -5.337951427775355, -5.524543751369389)
      )

      result zip validation foreach {
        case (arr: Array[Double], validRow: List[Double]) => assert(arr === validRow)
      }
    }
  }

  describe("Normalizer") {
    val path = modelPath("normalizer")

    it("should train") {
      val df = session.createDataFrame(Seq(
        (0, Vectors.dense(1.0, 0.5, -1.0)),
        (1, Vectors.dense(2.0, 1.0, 1.0)),
        (2, Vectors.dense(4.0, 10.0, 2.0))
      )).toDF("id", "features")

      val normalizer = new Normalizer()
        .setInputCol("features")
        .setOutputCol("normFeatures")
        .setP(1.0)

      val pipeline = new Pipeline().setStages(Array(normalizer))

      val model = pipeline.fit(df)

      model.write.overwrite().save(path)
    }

    it("should load") {
      PipelineLoader.load(path)
    }

    it("should transform") {
      val trainedModel = PipelineLoader.load(path)
      val data = LocalData(LocalDataColumn("features", List(
        List(1.0, 0.5, -1.0), List(2.0, 1.0, 1.0), List(4.0, 10.0, 2.0)
      )))
      val result = trainedModel.transform(data).column("normFeatures").get.data map { f =>
        f.asInstanceOf[DenseVector].toArray
      }
      val validation = Array(List(0.4,0.2,-0.4), List(0.5,0.25,0.25), List(0.25,0.625,0.125))

      result zip validation foreach {
        case (arr: Array[Double], validRow: List[Double]) => assert(arr === validRow)
      }
    }
  }

  describe("DCT") {
    val path = modelPath("dct")

    it("should train") {
      val data = Seq(
        Vectors.dense(0.0, 1.0, -2.0, 3.0),
        Vectors.dense(-1.0, 2.0, 4.0, -7.0),
        Vectors.dense(14.0, -2.0, -5.0, 1.0)
      )
      val df = session.createDataFrame(data.map(Tuple1.apply)).toDF("features")

      val dct = new DCT()
        .setInputCol("features")
        .setOutputCol("featuresDCT")
        .setInverse(false)

      val pipeline = new Pipeline().setStages(Array(dct))

      val model = pipeline.fit(df)

      model.write.overwrite().save(path)
    }

    it("should load") {
      PipelineLoader.load(path)
    }

    it("should transform") {
      val trainedModel = PipelineLoader.load(path)
      val data = LocalData(LocalDataColumn("features", List(
        List(0.0, 1.0, -2.0, 3.0),
        List(-1.0, 2.0, 4.0, -7.0),
        List(14.0, -2.0, -5.0, 1.0)
      )))
      val result = trainedModel.transform(data).column("featuresDCT").get.data map { f =>
        f.asInstanceOf[DenseVector].toArray
      }
      val validation = Array(
        List(1.0,-1.1480502970952693,2.0000000000000004,-2.7716385975338604),
        List(-1.0,3.378492794482933,-7.000000000000001,2.9301512653149677),
        List(4.0,9.304453421915744,11.000000000000002,1.5579302036357163)
      )

      result zip validation foreach {
        case (arr: Array[Double], validRow: List[Double]) => assert(arr === validRow)
      }
    }
  }

  describe("NaiveBayes") {
    val path = modelPath("naivebayes")

    it("should train") {
      val df = session.createDataFrame(Seq(
        (Vectors.dense(4.0, 0.2, 3.0, 4.0, 5.0), 1.0),
        (Vectors.dense(3.0, 0.3, 1.0, 4.1, 5.0), 1.0),
        (Vectors.dense(2.0, 0.5, 3.2, 4.0, 5.0), 1.0),
        (Vectors.dense(5.0, 0.7, 1.5, 4.0, 5.0), 1.0),
        (Vectors.dense(1.0, 0.1, 7.0, 4.0, 5.0), 0.0),
        (Vectors.dense(8.0, 0.3, 5.0, 1.0, 7.0), 0.0)
      )).toDF("features", "label")

      val nb = new NaiveBayes()

      val pipeline = new Pipeline().setStages(Array(nb))

      val model = pipeline.fit(df)

      model.write.overwrite().save(path)
    }

    it("should load") {
      PipelineLoader.load(path)
    }

    it("should transform") {
      val trainedModel = PipelineLoader.load(path)
      val data = LocalData(LocalDataColumn("features", List(
        List(4.0, 0.2, 3.0, 4.0, 5.0),
        List(3.0, 0.3, 1.0, 4.1, 5.0),
        List(1.0, 0.1, 7.0, 4.0, 5.0),
        List(8.0, 0.3, 5.0, 1.0, 7.0)
      )))
      val result = trainedModel.transform(data).column("prediction").get.data
      val validation = Array(1.0, 1.0, 0.0, 0.0)

      result zip validation foreach {
        case (arr: Double, valid: Double) => assert(arr === valid)
      }
    }
  }

  describe("Binarizer") {
    val path = modelPath("binarizer")
    val treshold = 5.0

    it("should load") {
      val data = Array((0, 0.1), (1, 0.8), (2, 0.2))
      val dataFrame = session.createDataFrame(data).toDF("id", "feature")

      val binarizer = new Binarizer()
        .setInputCol("feature")
        .setOutputCol("binarized_feature")
        .setThreshold(treshold)

      val pipeline = new Pipeline().setStages(Array(binarizer))

      pipeline.fit(dataFrame).write.overwrite().save(path)
      val model = PipelineLoader.load(path)
    }

    it("should transform") {
      val model = PipelineLoader.load(path)
      val localData = LocalData(LocalDataColumn("feature", List(0.1, 0.1, 5.9)))
      val result = model.transform(localData)
      val computedResults = result.column("feature").get.data.map {feature =>
        val f = feature.asInstanceOf[Double]
        if (f > treshold)
          1.0
        else
          0.0
      }
      val binarizerResults = result.column("binarized_feature").get.data.map(_.asInstanceOf[Double])
      assert(computedResults === binarizerResults)
    }
  }

  describe("Word2Vec") {
    val path = modelPath("word2vec")

    it("should train") {
      val documentDF = session.createDataFrame(Seq(
        "Hi I heard about Spark".split(" "),
        "I wish Java could use case classes".split(" "),
        "Logistic regression models are neat".split(" ")
      ).map(Tuple1.apply)).toDF("text")

      // Learn a mapping from words to Vectors.
      val word2Vec = new Word2Vec()
        .setInputCol("text")
        .setOutputCol("result")
        .setVectorSize(3)
        .setMinCount(0)
      val pipeline = new Pipeline().setStages(Array(word2Vec))

      val model = pipeline.fit(documentDF)

      model.write.overwrite().save(path)
    }

    it("should load") {
      PipelineLoader.load(path)
    }

    it("should transform") {
      val model = PipelineLoader.load(path)
      val validation = Array(-0.0024180402979254723, -0.016408352181315422, 0.017868943512439728)
      val localData = createInputData("text", "You know the rules and so do I".split(" ").toList)
      val result = model.transform(localData)
      val resultList = result.column("result").get.data.map(_.asInstanceOf[Array[Double]])
      compareArrDoubles(validation, resultList.head)
    }
  }

  describe("StringIndexer -> VectorIndexer -> DecisionTreeClassification -> IndexToString") {
    val path = modelPath("dtreeclassifier")

    it("should train") {
      val data = session.createDataFrame(Seq(
        (Vectors.dense(4.0, 0.2, 3.0, 4.0, 5.0), 1.0),
        (Vectors.dense(3.0, 0.3, 1.0, 4.1, 5.0), 1.0),
        (Vectors.dense(2.0, 0.5, 3.2, 4.0, 5.0), 1.0),
        (Vectors.dense(5.0, 0.7, 1.5, 4.0, 5.0), 1.0),
        (Vectors.dense(1.0, 0.1, 7.0, 4.0, 5.0), 0.0),
        (Vectors.dense(8.0, 0.3, 5.0, 1.0, 7.0), 0.0)
      )).toDF("features", "label")
      val labelIndexer = new StringIndexer()
        .setInputCol("label")
        .setOutputCol("indexedLabel")
        .fit(data)
      val featureIndexer = new VectorIndexer()
        .setInputCol("features")
        .setOutputCol("indexedFeatures")
        .setMaxCategories(4)// features with > 4 distinct values are treated as continuous.
        .fit(data)
      val dt = new DecisionTreeClassifier()
        .setLabelCol("indexedLabel")
        .setFeaturesCol("indexedFeatures")
      val labelConverter = new IndexToString()
        .setInputCol("prediction")
        .setOutputCol("predictedLabel")
        .setLabels(labelIndexer.labels)
      val pipeline = new Pipeline()
        .setStages(Array(labelIndexer, featureIndexer, dt, labelConverter))
      val model = pipeline.fit(data)
      model.write.overwrite().save(path)
    }

    it("should load") {
      val model = PipelineLoader.load(path)
    }

    it("should transform") {
      val model = PipelineLoader.load(path)
      val localData = createInputData("features", List(
        Array(1.0, 2.0, 3.0, 4.0, 5.0),
        Array(8.0, 0.0, 5.0, 1.0, 7.0)
      ))
      val result = model.transform(localData)
      val resLabels = result.column("predictedLabel").get.data.map(_.asInstanceOf[String])
      assert(resLabels === "1.0" :: "0.0" :: Nil)
    }
  }

  describe("VectorIndexer -> DecisionTreeRegression") {
    val path = modelPath("dtreeregressor")

    it("should train") {
      val data = session.createDataFrame(Seq(
        (Vectors.dense(4.0, 0.2, 3.0, 4.0, 5.0), 1.0),
        (Vectors.dense(3.0, 0.3, 1.0, 4.1, 5.0), 1.0),
        (Vectors.dense(2.0, 0.5, 3.2, 4.0, 5.0), 1.0),
        (Vectors.dense(5.0, 0.7, 1.5, 4.0, 5.0), 1.0),
        (Vectors.dense(1.0, 0.1, 7.0, 4.0, 5.0), 0.0),
        (Vectors.dense(8.0, 0.3, 5.0, 1.0, 7.0), 0.0)
      )).toDF("features", "label")
      val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(4).fit(data)
      // Train a DecisionTree model.
      val dt = new DecisionTreeRegressor().setLabelCol("label").setFeaturesCol("indexedFeatures")
      // Chain indexers and tree in a Pipeline.
      val pipeline = new Pipeline().setStages(Array(featureIndexer, dt))
      // Train model. This also runs the indexers.
      val model = pipeline.fit(data)
      model.write.overwrite().save(path)
    }

    it("should load") {
      val model = PipelineLoader.load(path)
    }

    it("should transform") {
      val model = PipelineLoader.load(path)
      val localData = createInputData("features", List(
        Array(1.0, 2.0, 3.0, 4.0, 5.0),
        Array(8.0, 0.0, 5.0, 1.0, 7.0)
      ))
      val result = model.transform(localData)
      val resLabels = result.column("prediction").get.data.map(_.asInstanceOf[Double])
      assert(resLabels === List(1.0, 0.0))
    }
  }

  describe("StringIndexer -> VectorIndexer -> RandomForestClassification -> IndexToString") {
    val path = modelPath("rforestclassifier")

    it("should train") {
      val data = session.createDataFrame(Seq(
        (Vectors.dense(4.0, 0.2, 3.0, 4.0, 5.0), 1.0),
        (Vectors.dense(3.0, 0.3, 1.0, 4.1, 5.0), 1.0),
        (Vectors.dense(2.0, 0.5, 3.2, 4.0, 5.0), 1.0),
        (Vectors.dense(5.0, 0.7, 1.5, 4.0, 5.0), 1.0),
        (Vectors.dense(1.0, 0.1, 7.0, 4.0, 5.0), 0.0),
        (Vectors.dense(8.0, 0.3, 5.0, 1.0, 7.0), 0.0)
      )).toDF("features", "label")

      val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(data)
      val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(4).fit(data)
      val rf = new RandomForestClassifier().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures").setNumTrees(10)
      val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)
      val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))
      val model = pipeline.fit(data)
      model.write.overwrite().save(path)
    }

    it("should load") {
      val model = PipelineLoader.load(path)
    }

    it("should transform") {
      val model = PipelineLoader.load(path)
      val localData = createInputData("features", List(
        Array(1.0, 2.0, 3.0, 4.0, 5.0),
        Array(8.0, 0.0, 5.0, 1.0, 7.0)
      ))
      val result = model.transform(localData)
      val resLabels = result.column("predictedLabel").get.data.map(_.asInstanceOf[String])
      assert(resLabels === "1.0" :: "0.0" :: Nil)
    }
  }

  describe("VectorIndexer -> RandomForestRegression") {
    val path = modelPath("rforestregression")

    it("should train") {
      val data = session.createDataFrame(Seq(
        (Vectors.dense(4.0, 0.2, 3.0, 4.0, 5.0), 1.0),
        (Vectors.dense(3.0, 0.3, 1.0, 4.1, 5.0), 1.0),
        (Vectors.dense(2.0, 0.5, 3.2, 4.0, 5.0), 1.0),
        (Vectors.dense(5.0, 0.7, 1.5, 4.0, 5.0), 1.0),
        (Vectors.dense(1.0, 0.1, 7.0, 4.0, 5.0), 0.0),
        (Vectors.dense(8.0, 0.3, 5.0, 1.0, 7.0), 0.0)
      )).toDF("features", "label")

      val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(4).fit(data)
      val rf = new RandomForestRegressor().setLabelCol("label").setFeaturesCol("indexedFeatures")
      val pipeline = new Pipeline().setStages(Array(featureIndexer, rf))
      val model = pipeline.fit(data)
      model.write.overwrite().save(path)
    }

    it("should load") {
      val model = PipelineLoader.load(path)
    }

    it("should transform") {
      val model = PipelineLoader.load(path)
      val localData = createInputData("features", List(
        Array(4.0, 0.2, 3.0, 4.0, 5.0),
        Array(3.0, 0.3, 1.0, 4.1, 5.0),
        Array(2.0, 0.5, 3.2, 4.0, 5.0),
        Array(5.0, 0.7, 1.5, 4.0, 5.0),
        Array(1.0, 0.1, 7.0, 4.0, 5.0),
        Array(8.0, 0.3, 5.0, 1.0, 7.0)
      ))
      val result = model.transform(localData)
      val resLabels = result.column("prediction").get.data.map(_.asInstanceOf[Double]).map(_ > 0.5)
      val refs = List(true, true, true, true, false, false)
      assert(resLabels === refs)
    }
  }

  describe("Tokenizer -> HashingTF -> LogisticRegression") {
    val path = modelPath("logisticregression")

    it("should train") {
      val training = session.createDataFrame(Seq(
        (0L, "a b c d e spark", 1.0),
        (1L, "b d", 0.0),
        (2L, "spark f g h", 1.0),
        (3L, "hadoop mapreduce", 0.0)
      )).toDF("id", "text", "label")
      val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
      val hashingTF = new HashingTF().setNumFeatures(1000).setInputCol(tokenizer.getOutputCol).setOutputCol("features")
      val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.01)
      val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, lr))
      val model = pipeline.fit(training)
      model.write.overwrite().save(path)
    }

    it("should load") {
      val model = PipelineLoader.load(path)
    }

    it("should transform") {
      val model = PipelineLoader.load(path)
      val localData = createInputData("text", List(
        "a b c d e spark",
        "b d",
        "spark f g h",
        "hadoop mapreduce"
      ))
      val result = model.transform(localData)
      assert(result.column("prediction").get.data === List(1.0, 0.0, 1.0, 0.0))
    }
  }

  describe("KMeans") {
    val path = modelPath("kmeans")

    it("should train") {
      val data = session.createDataFrame(Seq(
        (Vectors.dense(4.0, 0.2, 3.0, 4.0, 5.0), 1.0),
        (Vectors.dense(3.0, 0.3, 1.0, 4.1, 5.0), 1.0),
        (Vectors.dense(2.0, 0.5, 3.2, 4.0, 5.0), 1.0),
        (Vectors.dense(5.0, 0.7, 1.5, 4.0, 5.0), 1.0),
        (Vectors.dense(1.0, 0.1, 7.0, 4.0, 5.0), 0.0),
        (Vectors.dense(8.0, 0.3, 5.0, 1.0, 7.0), 0.0)
      )).toDF("features", "label")
      val kmeans = new KMeans().setK(2).setSeed(1L)
      val pipeline = new Pipeline().setStages(Array(kmeans))
      val model = pipeline.fit(data)
      model.write.overwrite().save(path)
    }
    it("should load") {
      val model = PipelineLoader.load(path)
    }
    it("should transform") {
      val model = PipelineLoader.load(path)
      val localData = createInputData("features", List(
        Array(4.0, 0.2, 3.0, 4.0, 5.0),
        Array(3.0, 0.3, 1.0, 4.1, 5.0),
        Array(2.0, 0.5, 3.2, 4.0, 5.0),
        Array(5.0, 0.7, 1.5, 4.0, 5.0),
        Array(1.0, 0.1, 7.0, 4.0, 5.0),
        Array(8.0, 0.3, 5.0, 1.0, 7.0)
      ))
      val result = model.transform(localData)
      val res = result.column("prediction").get.data.map(_.asInstanceOf[Int])
      val ref = List(0,0,0,0,1,0)
      res.zip(ref).foreach{
        case (resIdx, refIdx) => assert(resIdx === refIdx)
      }
    }
  }

  override def beforeAll {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("test")
      .set("spark.ui.enabled", "false")

    session = SparkSession.builder().config(conf).getOrCreate()
  }

  override def afterAll: Unit = {
    session.stop()
  }
}
