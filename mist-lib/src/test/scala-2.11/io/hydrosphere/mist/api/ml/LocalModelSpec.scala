package io.hydrosphere.mist.api.ml

import java.util.logging.LogManager

import org.apache.log4j.BasicConfigurator
import org.apache.spark.SparkConf
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.feature.{MaxAbsScaler, _}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSpec}
import LocalPipelineModel._


class LocalModelSpec extends FunSpec with BeforeAndAfterAll {
  var session: SparkSession = _

  def modelPath(modelName: String): String = s"./mist-lib/target/trained-models-for-test/$modelName"

  describe("CountVectorizer") {
    val path = modelPath("countvectorizer")

    it("should train") {
      import org.apache.spark.ml.feature.CountVectorizer

      val path = "./mist-lib/target/trained-models-for-test/countvectorizer"

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

    it("should load local/transform") {
      PipelineLoader.load(path)
    }

    it("should match") {
      val trainedModel = PipelineLoader.load(path)
      val data = LocalData(LocalDataColumn("words", List(List("a", "b", "c"))))
      val result = trainedModel.transform(data).column("features").get.data.map { f =>
        f.asInstanceOf[org.apache.spark.ml.linalg.SparseVector].toArray
      }
      val validation = List(List(1.0, 1.0, 1.0))

      result zip validation foreach {
        case (arr: Array[Double], validRow: List[Double]) => assert(arr === validRow)
      }
    }
  }

  describe("Word2Vec") {
    val path = modelPath("pca")

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
      trainedModel = PipelineLoader.load(path)
    }

    it("should match") {
      pending
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

    it("should load local/transform") {
      PipelineLoader.load(path)
    }


    it("should match") {
      pending
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

    it("should load local/transform") {
      trainedModel = PipelineLoader.load(path)
    }

    it("should match") {
      pending
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

    it("should load local/transform") {
      trainedModel = PipelineLoader.load(path)
    }

    it("should match") {
      pending
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

    it("should load local/transform") {
      trainedModel = PipelineLoader.load(path)
    }

    it("should match") {
      pending
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

    it("should load local/transform") {
      trainedModel = PipelineLoader.load(path)
    }

    it("should match") {
      pending
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
      trainedModel = PipelineLoader.load(path)
    }

    it("should match") {
      pending
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

    it("should load local/transform") {
      trainedModel = PipelineLoader.load(path)
    }

    it("should match") {
      pending
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
      trainedModel = PipelineLoader.load(path)
    }

    it("should match") {
      pending
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

    it("should load local/transform") {
      trainedModel = PipelineLoader.load(path)
    }

    it("should match") {
      pending
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
      trainedModel = PipelineLoader.load(path)
    }

    it("should match") {
      pending
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

  describe("IndexToString") {
    val path = modelPath("idx2str")
    it("should train") {
      val df = session.createDataFrame(Seq(
        (0, "a"),
        (1, "b"),
        (2, "c"),
        (3, "a"),
        (4, "a"),
        (5, "c")
      )).toDF("id", "category")

      val indexer = new StringIndexer()
        .setInputCol("category")
        .setOutputCol("categoryIndex")
        .fit(df)

      val converter = new IndexToString()
        .setInputCol("categoryIndex")
        .setOutputCol("originalCategory")

      val pipeline = new Pipeline().setStages(Array(indexer, converter))

      val model = pipeline.fit(df)

      model.write.overwrite().save("models/index")
    }
    it("should load") {pending}
    it("should transform") {pending}
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
