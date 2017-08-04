package io.hydrosphere.mist.jobs

import io.hydrosphere.mist.jobs.jar.JobsLoader
import org.scalatest.{Matchers, FunSpec}

class JobInfoSpec extends FunSpec with Matchers {

  describe("validation") {

    it("should accept pyJobs") {
      val info = PyJobInfo
      info.validateAction(Map.empty, Action.Execute).isRight shouldBe true
    }

    it("should validate jvmJobs") {
      val testJobClass = io.hydrosphere.mist.jobs.jar.MultiplyJob
      val jvmInfo = JvmJobInfo(JobsLoader.Common.loadJobClass(testJobClass.getClass.getCanonicalName).get)

      val r1 = jvmInfo.validateAction(Map.empty, Action.Execute)
      r1 should matchPattern {
        case Left(e: IllegalArgumentException) =>
      }

      val r2 = jvmInfo.validateAction(Map("numbers" -> List(1,2,3)), Action.Execute)
      r2.isRight shouldBe true
    }
  }
}
