package io.hydrosphere.mist

import org.scalatest.WordSpecLike
import io.hydrosphere.mist.jobs.FullJobConfiguration
import io.hydrosphere.mist.jobs.runners.python.wrappers.{ConfigurationWrapper, DataWrapper, ErrorWrapper}

class PythonWrappersTest extends WordSpecLike {
  "Configuration wrapper" must {
    "contain configuration" in {
      val jobConfiguration = FullJobConfiguration("path", "className", "contextName", Map("parameters" -> Seq(1,2,3)), Option("1"))
      val contextWrapper = new ConfigurationWrapper(jobConfiguration)

      assert( contextWrapper.className == jobConfiguration.className &&
              contextWrapper.parameters == jobConfiguration.parameters &&
              contextWrapper.path == jobConfiguration.path )
    }
  }

  "Data wrapper" must {
    "contain data" in {
      val data = Seq(1, 2, 3)
      val dataWrapper = new DataWrapper
      dataWrapper.set(data)

      assert(dataWrapper.get == data)
    }
  }

  "Errror wrapper" must {
    "contain error" in {
      val error = "Error"
      val errorWrapper = new ErrorWrapper
      errorWrapper.set(error)

      assert(errorWrapper.get() == error)
    }
  }
}
