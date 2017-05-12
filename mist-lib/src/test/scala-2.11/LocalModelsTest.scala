//
//import io.hydrosphere.mist.lib
//import io.hydrosphere.mist.contexts.ContextBuilder
//import io.hydrosphere.mist.jobs.runners.Runner
//import io.hydrosphere.mist.jobs.{FullJobConfiguration, MistJobConfiguration, ServingJobConfiguration}
//import io.hydrosphere.mist.utils.SparkUtils
//import io.hydrosphere.mist.utils.json.JobConfigurationJsonSerialization
//import org.apache.spark.mllib.linalg.{DenseVector, SparseVector => OldSparseVector, Vectors => OldVectors}
//import org.apache.spark.ml.linalg.{DenseVector => NewDenseVector, SparseVector => NewSparceVector}
//import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}
//import org.scalatest.concurrent.Eventually
//import spray.json.{DefaultJsonProtocol, pimpString}
//
//import scala.concurrent.duration._
//import collection.JavaConversions._
//
//
//class LocalModelsTest extends FunSuite with Eventually with BeforeAndAfterAll with JobConfigurationJsonSerialization with DefaultJsonProtocol {
//  val contextWrapper = ContextBuilder.namedSparkContext("foo")
//
//  override def beforeAll(): Unit = {
//    Thread.sleep(5000)
//  }
//
//  private def testServing(modelConfig: String)( predicate: (Map[String, Any] => Unit)): Unit = {
//    if(!SparkUtils.Version.areSessionsSupported)
//      cancel(s"Can't run in Spark ${SparkUtils.Version.sparkVersion}")
//    val json = modelConfig.parseJson
//    val jobConfiguration = json.convertTo[ServingJobConfiguration]
//    val serveJob = Runner(jobConfiguration, contextWrapper)
//    serveJob.run() match {
//      case Left(data) => predicate(data)
//      case Right(error) => assert(false, error)
//    }
//  }
//
//  private def extractResult[T](data: Map[String, Any]): List[Map[String, T]] = {
//    data("result").asInstanceOf[List[Map[String, T]]]
//  }
//
//  private def compareDoubles(data: Seq[Double], valid: Seq[Double]) = {
//    data zip valid foreach {
//      case (x: Double, y: Double) =>
//        assert(Math.abs(x - y) < 0.000001)
//    }
//  }
//
//  private def compareStrings(data: Seq[String], valid: Seq[String]) = {
//    data zip valid foreach {
//      case (x: String, y: String) => assert(x === y)
//    }
//  }
//
//  test("Local Binarizer test") {
//    testServing(TestConfig.LocalModels.binarizer) { data =>
//      val resList = extractResult[Double](data)
//      val threshold = 5.0
//      resList.foreach { x =>
//        val reference = if (x("feature") > threshold) 1.0 else 0.0
//        assert(reference === x("binarized_feature"))
//      }
//    }
//  }
//
//  test("Local Decision Tree Classification pipeline test") {
//    testServing(TestConfig.LocalModels.treeClassifier_1) { data =>
//      val resList = extractResult[Double](data)
//
//      resList foreach { map =>
//        assert(map("predictedLabel").toString.toDouble === 1.0)
//      }
//    }
//    testServing(TestConfig.LocalModels.treeClassifier_0) { data =>
//      val resList = extractResult[Double](data)
//      resList foreach { map =>
//        assert(map("predictedLabel").toString.toDouble === 0.0)
//      }
//    }
//  }
//
//  test("Local Decision Tree Regression pipeline test") {
//    testServing(TestConfig.LocalModels.treeRegressor_1) { data =>
//      val resList = extractResult[Double](data)
//      resList foreach { map =>
//        assert(map("prediction").toString.toDouble === 1.0)
//      }
//    }
//    testServing(TestConfig.LocalModels.treeRegressor_0) { data =>
//      val resList = extractResult[Double](data)
//      resList foreach { map =>
//        assert(map("prediction").toString.toDouble === 0.0)
//      }
//    }
//  }
//
//  test("Local Random Forest Classification pipeline test") {
//    testServing(TestConfig.LocalModels.forestClassifier_1) { data =>
//      val resList = extractResult[Double](data)
//      resList foreach { map =>
//        assert(map("predictedLabel").toString.toDouble === 1.0)
//      }
//    }
//    testServing(TestConfig.LocalModels.forestClassifier_0) { data =>
//      val resList = extractResult[Double](data)
//      resList foreach { map =>
//        assert(map("predictedLabel").toString.toDouble === 0.0)
//      }
//    }
//  }
//
//  test("Local Random Forest Regression pipeline test") {
//    testServing(TestConfig.LocalModels.forestRegressor_1) { data =>
//      val resList = extractResult[Double](data)
//      resList foreach { map =>
//        assert(map("prediction").toString.toDouble === 1.0)
//      }
//    }
//    testServing(TestConfig.LocalModels.forestRegressor_0) { data =>
//      val resList = extractResult[Double](data)
//      resList foreach { map =>
//        assert(map("prediction").toString.toDouble === 0.0)
//      }
//    }
//  }
//
//  test("Local PCA test") {
//    testServing(TestConfig.LocalModels.pca) { data =>
//      val validation = Array(
//        List(-4.645104331781534, -1.1167972663619026, -5.524543751369387),
//        List(-6.428880535676489, -5.337951427775355, -5.524543751369389)
//      )
//      val resList = extractResult[Any](data) map(x => x("pcaFeatures").asInstanceOf[DenseVector].toArray)
//
//      resList zip validation foreach {
//        case (arr: Array[Double], validRow: List[Double]) =>
//          compareDoubles(arr, validRow)
//      }
//    }
//  }
//
//  test("Local MinMaxScaler test") {
//    testServing(TestConfig.LocalModels.minMaxScaler) { data =>
//      val validation = Array(
//        List(0.0, -0.01, 0.0),
//        List(0.5, 0.13999999999999999, 0.5),
//        List(1.0, 0.1, 1.0)
//      )
//      val resList = extractResult[Any](data) map(x => x("scaledFeatures").asInstanceOf[Array[Double]])
//
//      resList zip validation foreach {
//        case (arr: Array[Double], validRow: List[Double]) =>
//          compareDoubles(arr, validRow)
//      }
//    }
//  }
//
//  test("Local StandardScaler test") {
//    testServing(TestConfig.LocalModels.standardScaler) { data =>
//      val validation = Array(
//        List(0.5, 0.0, 0.6546536707079772, 1.7320508075688774, 0.0),
//        List(1.0, 0.0, 1.9639610121239315, 3.464101615137755, 4.330127018922194),
//        List(2.0, 0.0, 0.0, 5.196152422706632, 6.062177826491071)
//      )
//      val resList = extractResult[Any](data) map(x => x("scaledFeatures").asInstanceOf[DenseVector].toArray)
//
//      resList zip validation foreach {
//        case (arr: Array[Double], validRow: List[Double]) =>
//          compareDoubles(arr, validRow)
//      }
//    }
//  }
//
//  test("Local MaxAbsScaler test") {
//    testServing(TestConfig.LocalModels.maxAbsScaler) { data =>
//      val validation = Array(
//        List(0.25, 0.0, 0.125),
//        List(0.5, 0.4, 0.625),
//        List(0.0, 0.6, 0.875)
//      )
//      val resList = extractResult[Any](data) map(x => x("scaledFeatures").asInstanceOf[NewDenseVector].toArray)
//
//      resList zip validation foreach {
//        case (arr: Array[Double], validRow: List[Double]) =>
//        compareDoubles(arr, validRow)
//      }
//    }
//  }
//
//  test("Local StringIndexer test") {
//    testServing(TestConfig.LocalModels.stringIndexer) { data =>
//      val validation = Array(0.0, 2.0, 1.0)
//      val resList = extractResult[Double](data) map(x => x("categoryIndex"))
//
//      resList zip validation foreach {
//        case (value: Double, validVal) =>
//          assert(value === validVal)
//      }
//    }
//  }
//
//  test("Local OneHotEncoder pipeline test") {
//    testServing(TestConfig.LocalModels.oneHotEncoder) { data =>
//      val validation = Array(List(1.0, 0.0), List(0.0, 0.0), List(0.0, 1.0), List(1.0, 0.0))
//      val resList = extractResult[Any](data) map(x => x("categoryVec").asInstanceOf[NewSparceVector].toArray)
//
//      resList zip validation foreach {
//        case (arr: Array[Double], validRow: List[Double]) =>
//          compareDoubles(arr, validRow)
//      }
//    }
//  }
//
//  test("Local NGram test") {
//    testServing(TestConfig.LocalModels.ngram) { data =>
//      val validation = Array(List("Provectus team", "team is", "is awesome"))
//      val resList = extractResult[Any](data) map(x => x("ngrams").asInstanceOf[Seq[String]].toArray)
//
//      resList zip validation foreach {
//        case (arr: Array[String], validRow: List[String]) => compareStrings(arr, validRow)
//      }
//    }
//  }
//
//  test("Local StopWordsRemover test") {
//    testServing(TestConfig.LocalModels.stopwordsremover) { data =>
//      val validation = Array(List("saw", "red", "balloon"), List("Mary", "little", "lamb"))
//      val resList = extractResult[Any](data) map(x => x("filtered").asInstanceOf[Seq[String]].toArray)
//
//      resList zip validation foreach {
//        case (arr: Array[String], validRow: List[String]) => compareStrings(arr, validRow)
//      }
//    }
//  }
//
//  test("Local Normalizer test") {
//    testServing(TestConfig.LocalModels.normalizer) { data =>
//      val validation = Array(List(0.4,0.2,-0.4), List(0.5,0.25,0.25), List(0.25,0.625,0.125))
//      val resList = extractResult[Any](data) map(x => x("normFeatures").asInstanceOf[NewDenseVector].toArray)
//
//      resList zip validation foreach {
//        case (arr: Array[Double], validRow: List[Double]) => compareDoubles(arr, validRow)
//      }
//    }
//  }
//
//  test("Local PolynomialExpansion test") {
//    testServing(TestConfig.LocalModels.polynomialExpansion) { data =>
//      val validation = Array(
//        List(2.0,4.0,8.0,1.0,2.0,4.0,1.0,2.0,1.0),
//        List(0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0),
//        List(3.0,9.0,27.0,-1.0,-3.0,-9.0,1.0,3.0,-1.0)
//      )
//      val resList = extractResult[Any](data) map(x => x("polyFeatures").asInstanceOf[NewDenseVector].toArray)
//      resList zip validation foreach {
//        case (arr: Array[Double], validRow: List[Double]) => compareDoubles(arr, validRow)
//      }
//    }
//  }
//
//  test("Local DCT test") {
//    testServing(TestConfig.LocalModels.dct) { data =>
//      val validation = Array(
//        List(1.0,-1.1480502970952693,2.0000000000000004,-2.7716385975338604),
//        List(-1.0,3.378492794482933,-7.000000000000001,2.9301512653149677),
//        List(4.0,9.304453421915744,11.000000000000002,1.5579302036357163)
//      )
//      val resList = extractResult[Any](data) map(x => x("featuresDCT").asInstanceOf[NewDenseVector].toArray)
//      resList zip validation foreach {
//        case (arr: Array[Double], validRow: List[Double]) => compareDoubles(arr, validRow)
//      }
//    }
//  }
//
//  test("Local NaiveBayes test") {
//    testServing(TestConfig.LocalModels.naiveBayes) { data =>
//      val validation = Array(1.0, 1.0, 0.0, 0.0)
//      val resList = extractResult[Any](data) map(x => x("prediction").asInstanceOf[Double])
//      compareDoubles(resList, validation)
//    }
//  }
//
//  test("Local TFIDF test") {
//    testServing(TestConfig.LocalModels.tfidf) { data =>
//      val validation = OldVectors.sparse(
//        20,
//        Array(3,10,13,18),
//        Array(2.772588722239781,1.3862943611198906,1.3862943611198906,0.6931471805599453)
//      )
//      val resList = extractResult[Any](data) map(x => x("features").asInstanceOf[OldSparseVector])
//      assert(validation.equals(resList.get(0)))
//    }
//  }
//
//  test("Local GBTRegression test") {
//    testServing(TestConfig.LocalModels.gbtregressor) { data =>
//      val resList = extractResult[Any](data) map(x => x("prediction").asInstanceOf[Double])
//      resList.asInstanceOf[List[Double]].foreach(x => assert(x != 0.0))
//    }
//  }
//
//  override def afterAll(): Unit ={
//    contextWrapper.stop()
//
//    Thread.sleep(5000)
//  }
//}
