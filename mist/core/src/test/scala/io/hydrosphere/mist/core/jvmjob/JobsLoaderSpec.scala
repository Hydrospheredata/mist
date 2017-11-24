package io.hydrosphere.mist.core.jvmjob

import io.hydrosphere.mist.core.CommonData.Action
import org.scalatest.{FunSpec, Matchers}

class JobsLoaderSpec extends FunSpec with Matchers {

  it("should load specific instance") {
    val r = JobsLoader.Common.loadJobInstance(className(MultiplyJob), Action.Execute)
    r.isSuccess shouldBe true
  }

  it("should fail not implemented instance") {
    val r = JobsLoader.Common.loadJobInstance(className(MultiplyJob), Action.Serve)
    r.isSuccess shouldBe false
  }

  def className(any: Any): String = any.getClass.getCanonicalName
}
