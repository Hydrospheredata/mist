package io.hydrosphere.mist.jobs

import java.io.File

import io.hydrosphere.mist.core.CommonData.Action
import io.hydrosphere.mist.core.MockitoSugar
import io.hydrosphere.mist.core.jvmjob.{ExtractedFunctionData, FunctionInfoData, FunctionInstanceLoader, OldInstanceWrapper}
import io.hydrosphere.mist.job._
import io.hydrosphere.mist.utils.{Err, Succ}
import mist.api.args.{InternalArgument, MInt, UserInputArgument}
import mist.api.internal.{JavaFunctionInstance, FunctionInstance, ScalaFunctionInstance}
import org.mockito.Matchers.{endsWith => mockitoEndsWith, eq => mockitoEq}
import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

import scala.util.{Failure, Success}

class FunctionInfoExtractorSpec extends FunSpecLike
  with Matchers
  with MockitoSugar
  with BeforeAndAfterAll {

  it("should create BaseJobInfoExtractor") {
    FunctionInfoExtractor()
  }

  describe("BaseJobInfoExtractor") {
    it("should extract jvm job") {
      val jvmExtractor = mock[JvmFunctionInfoExtractor]
      val pyExtractor = mock[PythonFunctionInfoExtractor]

      when(jvmExtractor.extractInfo(any[File], any[String]))
        .thenReturn(Succ(FunctionInfo(data = ExtractedFunctionData("test"))))

      val baseJobInfoExtractor = new BaseFunctionInfoExtractor(jvmExtractor, pyExtractor)

      val info = baseJobInfoExtractor.extractInfo(new File("doesnt_matter.jar"), "Test")

      info.get shouldBe FunctionInfo(data = ExtractedFunctionData("test"))

      verify(jvmExtractor, times(1)).extractInfo(any[File], any[String])
      verify(pyExtractor, never()).extractInfo(any[File], any[String])
    }
    it("should extract py job") {
      val jvmExtractor = mock[JvmFunctionInfoExtractor]
      val pyExtractor = mock[PythonFunctionInfoExtractor]

      when(pyExtractor.extractInfo(any[File], any[String]))
        .thenReturn(Succ(FunctionInfo(data = ExtractedFunctionData("test"))))

      val baseJobInfoExtractor = new BaseFunctionInfoExtractor(jvmExtractor, pyExtractor)

      val info = baseJobInfoExtractor.extractInfo(new File("doesnt_matter.py"), "Test")

      info.get shouldBe FunctionInfo(data = ExtractedFunctionData("test"))

      verify(jvmExtractor, never()).extractInfo(any[File], any[String])
      verify(pyExtractor, times(1)).extractInfo(any[File], any[String])

    }
  }

  describe("JvmJobInfoExtractor") {


    it("should create JvmJobExtractor") {
      JvmFunctionInfoExtractor()
    }

    it("should extract job info prior to language") {
      val jobsLoader = mock[FunctionInstanceLoader]
      val scalaJobInstance = mock[ScalaFunctionInstance]
      val javaJobInstance = mock[JavaFunctionInstance]
      val oldJobInstance = mock[OldInstanceWrapper]

      val jvmJobInfoExtractor = new JvmFunctionInfoExtractor(_ => jobsLoader)
      when(jobsLoader.loadFnInstance(mockitoEndsWith("Scala"), any[Action]))
        .thenReturn(Succ(scalaJobInstance))

      when(jobsLoader.loadFnInstance(mockitoEndsWith("Java"), any[Action]))
        .thenReturn(Succ(javaJobInstance))

      when(jobsLoader.loadFnInstance(mockitoEndsWith("Old"), any[Action]))
        .thenReturn(Succ(oldJobInstance))

      when(scalaJobInstance.describe())
        .thenReturn(Seq(
          UserInputArgument("num", MInt),
          InternalArgument()
        ))
      when(javaJobInstance.describe())
        .thenReturn(Seq(
          UserInputArgument("num", MInt),
          InternalArgument()
        ))
      when(oldJobInstance.describe())
        .thenReturn(Seq(
          UserInputArgument("num", MInt),
          InternalArgument()
        ))

      val res = jvmJobInfoExtractor.extractInfo(new File("doesnt_matter"), "TestJava")
      res.isSuccess shouldBe true
      res.get.data shouldBe ExtractedFunctionData(
        lang = "java",
        execute=Seq(UserInputArgument("num", MInt))
      )
      val scalaJob = jvmJobInfoExtractor.extractInfo(new File("doesnt_matter"), "TestScala")

      scalaJob.isSuccess shouldBe true
      scalaJob.get.data shouldBe ExtractedFunctionData(
        lang = "scala",
        execute=Seq(UserInputArgument("num", MInt))
      )
      val otherJob = jvmJobInfoExtractor.extractInfo(new File("doesnt_matter"), "TestOld")

      otherJob.isSuccess shouldBe true
      otherJob.get.data shouldBe ExtractedFunctionData(
        lang = "scala",
        execute=Seq(UserInputArgument("num", MInt))
      )
    }

    it("should return failure then jobsloader fails load instance") {
      val jobsLoader = mock[FunctionInstanceLoader]
      when(jobsLoader.loadFnInstance(any[String], any[Action]))
        .thenReturn(Err(new IllegalArgumentException("invalid")))
      val jvmJobInfoExtractor = new JvmFunctionInfoExtractor(_ => jobsLoader)

      val res = jvmJobInfoExtractor.extractInfo(new File("doesnt_matter"), "Rest")

      res.isFailure shouldBe true
    }

    it("should load job instance for old serve method") {
      val oldInstance = mock[OldInstanceWrapper]
      val jobsLoader = mock[FunctionInstanceLoader]
      when(jobsLoader.loadFnInstance(any[String], mockitoEq(Action.Execute)))
        .thenReturn(Err(new IllegalArgumentException("invalid")))
      when(oldInstance.describe())
        .thenReturn(Seq(
          UserInputArgument("num", MInt),
          InternalArgument()
        ))
      when(jobsLoader.loadFnInstance(any[String], mockitoEq(Action.Serve)))
        .thenReturn(Succ(oldInstance))
      val jvmJobInfoExtractor = new JvmFunctionInfoExtractor(_ => jobsLoader)

      val res = jvmJobInfoExtractor.extractInfo(new File("doesnt_matter"), "TestClass")

      res.isSuccess shouldBe true
      res.get.data shouldBe ExtractedFunctionData(
        lang="scala",
        execute = Seq(UserInputArgument("num", MInt)),
        isServe = true
      )
    }

    it("should get tags from internal arguments"){
      val javaJobInstance = mock[JavaFunctionInstance]
      val jobsLoader = mock[FunctionInstanceLoader]

      when(jobsLoader.loadFnInstance(mockitoEndsWith("Java"), any[Action]))
        .thenReturn(Succ(javaJobInstance))

      when(javaJobInstance.describe())
        .thenReturn(Seq(
          UserInputArgument("num", MInt),
          InternalArgument(Seq("testTag"))
        ))
      val jvmJobInfoExtractor = new JvmFunctionInfoExtractor(_ => jobsLoader)

      val res = jvmJobInfoExtractor.extractInfo(new File("doesnt_matter"), "TestJava")
      res.isSuccess shouldBe true
      res.get.data shouldBe ExtractedFunctionData(
        lang = "java",
        execute = Seq(UserInputArgument("num", MInt)),
        tags = Seq("testTag")
      )

    }

  }
  describe("PyJobInfoExtractor") {
    it("should create py job info extractor") {
      new PythonFunctionInfoExtractor
    }

    it("should extract py info") {
      val pythonJobInfoExtractor = new PythonFunctionInfoExtractor
      val res = pythonJobInfoExtractor.extractInfo(new File("doesnt_matter"), "Test")
      res.isSuccess shouldBe true
      res.get.data shouldBe ExtractedFunctionData(
        lang="python"
      )
      res.get.instance shouldBe FunctionInstance.NoOpInstance
    }
  }




}
