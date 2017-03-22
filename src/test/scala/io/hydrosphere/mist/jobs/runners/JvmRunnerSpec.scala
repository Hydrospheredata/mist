package io.hydrosphere.mist.jobs.runners

import com.holdenkarau.spark.testing.SharedSparkContext
import io.hydrosphere.mist.api.MistJob
import io.hydrosphere.mist.jobs.Action
import io.hydrosphere.mist.jobs.runners.jar.{JvmJobInstance, JvmJobLoader}
import io.hydrosphere.mist.lib.spark1.SetupConfiguration
import org.apache.spark.streaming.Duration
import org.scalatest.{FunSpec, Inside, Matchers}

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
  }

  def className(any: Any): String = any.getClass.getCanonicalName

  describe("mist job instance") {

    it("should execute") {
      val clz = MultiplyJob.getClass
      val method = clz.getMethods.find(_.getName == "execute").get
      val instance = new JvmJobInstance(clz, method)

      val conf = new SetupConfiguration(sc, Duration(10), "", "")
      val result = instance.run(conf, Map("numbers" -> List(1,2,4)))

      result shouldBe Right(Map("r" -> List(2,4,8)))
    }

    it("should apply optional params correctly") {
      val clz = OptParamJob.getClass
      val method = clz.getMethods.find(_.getName == "execute").get

      val conf = new SetupConfiguration(sc, Duration(10), "", "")
      val instance = new JvmJobInstance(clz, method)

      instance.run(conf, Map("p" -> 1)) shouldBe Right(Map("r" -> 1))
      instance.run(conf, Map.empty) shouldBe Right(Map("r" -> 42))
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
