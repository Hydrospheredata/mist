package io.hydrosphere.mist

//import scala.collection.JavaConverters._
//import org.scalatest.WordSpecLike
//import io.hydrosphere.mist.jobs.MistJobConfiguration
//import io.hydrosphere.mist.jobs.runners.python.wrappers.{ConfigurationWrapper, DataWrapper, ErrorWrapper}
//
//class PythonWrappersTest extends WordSpecLike {
//  "Configuration wrapper" must {
//    "contain configuration" in {
//      val jobConfiguration = MistJobConfiguration("path", "className", "contextName", Map("parameters" -> Seq(1,2,3)), Option("1"))
//      val contextWrapper = new ConfigurationWrapper(jobConfiguration)
//
//      val javaParameters = new java.util.HashMap[String, Any](1)
//      javaParameters.put("parameters", Seq(1, 2, 3).asJava)
//
//      assert( contextWrapper.className == jobConfiguration.className &&
//              contextWrapper.parameters == javaParameters &&
//              contextWrapper.path == jobConfiguration.path )
//    }
//  }
//
//  "Data wrapper" must {
//    "contain data" in {
//      val data = new java.util.HashMap[String, Any](1)
//      data.put("result", Seq(1, 2, 3).asJava)
//      val dataWrapper = new DataWrapper
//      dataWrapper.set(data)
//
//      assert(dataWrapper.get == Map("result" -> List(1, 2, 3)))
//    }
//  }
//
//  "Errror wrapper" must {
//    "contain error" in {
//      val error = "Error"
//      val errorWrapper = new ErrorWrapper
//      errorWrapper.set(error)
//
//      assert(errorWrapper.get() == error)
//    }
//  }
//}
