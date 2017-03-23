package io.hydrosphere.mist.jobs.runners

import java.nio.file.{Paths, Files}

import com.holdenkarau.spark.testing.SharedSparkContext
import io.hydrosphere.mist.api._
import io.hydrosphere.mist.jobs.Action
import io.hydrosphere.mist.jobs.runners.jar.{JvmJobInstance, JvmJobLoader}
import org.apache.spark.streaming.Duration
import org.scalatest.{FunSpec, Inside, Matchers}

import scala.io.Source
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

class JvmRunnerSpec extends FunSpec with SharedSparkContext with Matchers with Inside {

  val classLoader = getClass.getClassLoader

  describe("JvmJobLoader") {

    it("should load MistJob") {
      val r = JvmJobLoader.load(className(MultiplyJob), Action.Execute, classLoader)
      r.isSuccess shouldBe true

      val instance = r.get
      instance.arguments shouldBe Map("numbers" -> typeOf[List[Int]])
    }

    it("should fail to load invalid job") {
      val r = JvmJobLoader.load(className(InvalidJob), Action.Execute, classLoader)
      r.isSuccess shouldBe false
    }

    it("should load jar") {
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
      val r = JvmJobLoader.loadFromJar("MyJob$", Action.Execute, jar)
      r.isSuccess shouldBe true
    }

    def className(any: Any): String = any.getClass.getCanonicalName
  }


  describe("mist job instance") {

    it("should execute") {
      val instance = instanceFor[MultiplyJob.type]
      val conf = new SetupConfiguration(sc, Duration(10), "", "")

      // valid params
      instance.run(conf, Map("numbers" -> List(1,2,4))) shouldBe Right(Map("r" -> List(2,4,8)))
      // invalid params
      instance.run(conf, Map.empty).isLeft shouldBe true
    }

    it("should apply optional params correctly") {
      val instance = instanceFor[OptParamJob.type]
      val conf = new SetupConfiguration(sc, Duration(10), "", "")

      instance.run(conf, Map("p" -> 1)) shouldBe Right(Map("r" -> 1))
      instance.run(conf, Map.empty) shouldBe Right(Map("r" -> 42))
    }

    def instanceFor[A](implicit tag: ClassTag[A]): JvmJobInstance = {
      val clz = tag.runtimeClass
      val method = clz.getMethods.find(_.getName == "execute").get
      new JvmJobInstance(clz, method)
    }
  }
}

object MultiplyJob extends MistJob {

  def execute(numbers: List[Int]): Map[String, Any] = {
    val rdd = context.parallelize(numbers)
    Map("r" -> rdd.map(x => x * 2).collect().toList)
  }
}

object OptParamJob extends MistJob {

  def execute(p: Option[Int]): Map[String, Any] = {
    Map("r" -> p.getOrElse(42))
  }
}

object InvalidJob extends MistJob
