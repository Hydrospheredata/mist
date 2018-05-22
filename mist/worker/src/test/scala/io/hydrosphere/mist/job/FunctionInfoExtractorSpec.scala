package io.hydrosphere.mist.job

import java.io.File

import io.hydrosphere.mist.core.CommonData.{Action, EnvInfo}
import io.hydrosphere.mist.core.{ExtractedFunctionData, MockitoSugar, PythonEntrySettings}
import io.hydrosphere.mist.utils.{Err, Succ}
import mist.api.{InternalArgument, MInt, UserInputArgument}
import org.mockito.Matchers.{endsWith => mockitoEndsWith, eq => mockitoEq}
import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

class FunctionInfoExtractorSpec extends FunSpecLike
  with Matchers
  with MockitoSugar
  with BeforeAndAfterAll {

  it("should create BaseJobInfoExtractor") {
    FunctionInfoExtractor()
  }

  val envInfo = EnvInfo(PythonEntrySettings("python", "python"))

  describe("BaseJobInfoExtractor") {
    it("should extract jvm job") {
      val jvmExtractor = mock[JvmFunctionInfoExtractor]
      val pyExtractor = mock[PythonFunctionInfoExtractor]

      when(jvmExtractor.extractInfo(any[File], any[String], any[EnvInfo]))
        .thenReturn(Succ(FunctionInfo(data = ExtractedFunctionData("test"))))

      val baseJobInfoExtractor = new BaseFunctionInfoExtractor(jvmExtractor, pyExtractor)

      val info = baseJobInfoExtractor.extractInfo(new File("doesnt_matter.jar"), "Test", envInfo)

      info.get shouldBe FunctionInfo(data = ExtractedFunctionData("test"))

      verify(jvmExtractor, times(1)).extractInfo(any[File], any[String], any[EnvInfo])
      verify(pyExtractor, never()).extractInfo(any[File], any[String], any[EnvInfo])
    }
    it("should extract py job") {
      val jvmExtractor = mock[JvmFunctionInfoExtractor]
      val pyExtractor = mock[PythonFunctionInfoExtractor]

      when(pyExtractor.extractInfo(any[File], any[String], any[EnvInfo]))
        .thenReturn(Succ(FunctionInfo(data = ExtractedFunctionData("test"))))

      val baseJobInfoExtractor = new BaseFunctionInfoExtractor(jvmExtractor, pyExtractor)

      val info = baseJobInfoExtractor.extractInfo(new File("doesnt_matter.py"), "Test", envInfo)

      info.get shouldBe FunctionInfo(data = ExtractedFunctionData("test"))

      verify(jvmExtractor, never()).extractInfo(any[File], any[String], any[EnvInfo])
      verify(pyExtractor, times(1)).extractInfo(any[File], any[String], any[EnvInfo])

    }
  }

  describe("JvmJobInfoExtractor") {


    it("should create JvmJobExtractor") {
      JvmFunctionInfoExtractor()
    }

    it("should extract job info prior to language") {
      val jobsLoader = mock[FunctionInstanceLoader]
      val scalaJobInstance = mock[JvmFunctionInstance]
      val javaJobInstance = mock[JvmFunctionInstance]

      val jvmJobInfoExtractor = new JvmFunctionInfoExtractor(_ => jobsLoader)
      when(jobsLoader.loadFnInstance(mockitoEndsWith("Scala"), any[Action]))
        .thenReturn(Succ(scalaJobInstance))

      when(jobsLoader.loadFnInstance(mockitoEndsWith("Java"), any[Action]))
        .thenReturn(Succ(javaJobInstance))

      when(scalaJobInstance.describe())
        .thenReturn(Seq(
          UserInputArgument("num", MInt),
          InternalArgument()
        ))
      when(scalaJobInstance.lang).thenReturn("scala")

      when(javaJobInstance.describe())
        .thenReturn(Seq(
          UserInputArgument("num", MInt),
          InternalArgument()
        ))
      when(javaJobInstance.lang).thenReturn("java")

      val res = jvmJobInfoExtractor.extractInfo(new File("doesnt_matter"), "TestJava", envInfo)
      res.isSuccess shouldBe true
      res.get.data shouldBe ExtractedFunctionData(
        lang = "java",
        execute=Seq(UserInputArgument("num", MInt))
      )
      val scalaJob = jvmJobInfoExtractor.extractInfo(new File("doesnt_matter"), "TestScala", envInfo)

      scalaJob.isSuccess shouldBe true
      scalaJob.get.data shouldBe ExtractedFunctionData(
        lang = "scala",
        execute=Seq(UserInputArgument("num", MInt))
      )
    }

    it("should return failure then jobsloader fails load instance") {
      val jobsLoader = mock[FunctionInstanceLoader]
      when(jobsLoader.loadFnInstance(any[String], any[Action]))
        .thenReturn(Err(new IllegalArgumentException("invalid")))
      val jvmJobInfoExtractor = new JvmFunctionInfoExtractor(_ => jobsLoader)

      val res = jvmJobInfoExtractor.extractInfo(new File("doesnt_matter"), "Rest", envInfo)

      res.isFailure shouldBe true
    }

    it("should get tags from internal arguments"){
      val jobInstance = mock[JvmFunctionInstance]
      val jobsLoader = mock[FunctionInstanceLoader]

      when(jobsLoader.loadFnInstance(mockitoEndsWith("Java"), any[Action]))
        .thenReturn(Succ(jobInstance))

      when(jobInstance.describe())
        .thenReturn(Seq(
          UserInputArgument("num", MInt),
          InternalArgument(Seq("testTag"))
        ))
      when(jobInstance.lang).thenReturn("java")

      val jvmJobInfoExtractor = new JvmFunctionInfoExtractor(_ => jobsLoader)

      val res = jvmJobInfoExtractor.extractInfo(new File("doesnt_matter"), "TestJava", envInfo)
      res.isSuccess shouldBe true
      res.get.data shouldBe ExtractedFunctionData(
        lang = "java",
        execute = Seq(UserInputArgument("num", MInt)),
        tags = Seq("testTag")
      )

    }

  }

  describe("PyJobInfoExtractor") {

    it("should extract py info") {
      val pythonJobInfoExtractor =
        new PythonFunctionInfoExtractor((_, _, _) => Right(Seq(UserInputArgument("x", MInt))))

      val res = pythonJobInfoExtractor.extractInfo(new File("doesnt_matter"), "Test", envInfo)
      res.isSuccess shouldBe true
      res.get.data shouldBe ExtractedFunctionData(
        name = "Test",
        lang="python",
        execute = Seq(UserInputArgument("x", MInt))
      )
    }
  }

}
