package io.hydrosphere.mist.jobs.jar

import java.nio.file.Paths

import io.hydrosphere.mist.jobs.Action
import org.scalatest.{FunSpec, Matchers}

class JobsLoaderSpec extends FunSpec with Matchers {

  it("should load MistJob") {
    val r = JobsLoader.Common.loadJobClass(className(MultiplyJob))
    r.isSuccess shouldBe true

    val jobClass = r.get

    jobClass.execute.isDefined shouldBe true
    jobClass.serve.isDefined shouldBe false
  }

  it("should load specific instance") {
    val r = JobsLoader.Common.loadJobInstance(className(MultiplyJob), Action.Execute)
    r.isSuccess shouldBe true
  }

  it("should fail not implemeted instacne") {
    val r = JobsLoader.Common.loadJobInstance(className(MultiplyJob), Action.Serve)
    r.isSuccess shouldBe false
  }

  it("should load from jar") {
    val source =s"""
       |import io.hydrosphere.mist.api._
       |
       |object MyJob extends MistJob {
       |
       |  def execute(n: List[Int]): Map[String, Any] = {
       |    val rdd = context.parallelize(n)
       |    Map("r" -> rdd.map(x => x * 2).collect().toList)
       |  }
       |}""".stripMargin

    val targetDir = "target/jar_load_test"
    val compiler = new TestCompiler(targetDir)
    compiler.compile(source, "MyJob")

    JarPackager.pack(targetDir, targetDir)

    val jar = Paths.get("target/jar_load_test/jar_load_test.jar").toFile
    val r = JobsLoader.fromJar(jar).loadJobClass("MyJob$")
    r.isSuccess shouldBe true
  }

  def className(any: Any): String = any.getClass.getCanonicalName
}
