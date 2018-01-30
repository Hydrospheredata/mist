package io.hydrosphere.mist.jobs

import java.io.File

import io.hydrosphere.mist.core.CommonData.Action
import io.hydrosphere.mist.core.MockitoSugar
import io.hydrosphere.mist.core.jvmjob.{ExtractedData, JobInfoData, JobsLoader, OldInstanceWrapper}
import io.hydrosphere.mist.job._
import io.hydrosphere.mist.utils.{Err, Succ}
import mist.api.args.{InternalArgument, MInt, UserInputArgument}
import mist.api.internal.{JavaJobInstance, JobInstance, ScalaJobInstance}
import org.mockito.Matchers.{endsWith => mockitoEndsWith, eq => mockitoEq}
import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

import scala.util.{Failure, Success}

class JobInfoExtractorSpec extends FunSpecLike
  with Matchers
  with MockitoSugar
  with BeforeAndAfterAll {

  it("should create BaseJobInfoExtractor") {
    JobInfoExtractor()
  }

  describe("BaseJobInfoExtractor") {
    it("should extract jvm job") {
      val jvmExtractor = mock[JvmJobInfoExtractor]
      val pyExtractor = mock[PythonJobInfoExtractor]

      when(jvmExtractor.extractInfo(any[File], any[String]))
        .thenReturn(Succ(JobInfo(data = ExtractedData("test"))))

      val baseJobInfoExtractor = new BaseJobInfoExtractor(jvmExtractor, pyExtractor)

      val info = baseJobInfoExtractor.extractInfo(new File("doesnt_matter.jar"), "Test")

      info.get shouldBe JobInfo(data = ExtractedData("test"))

      verify(jvmExtractor, times(1)).extractInfo(any[File], any[String])
      verify(pyExtractor, never()).extractInfo(any[File], any[String])
    }
    it("should extract py job") {
      val jvmExtractor = mock[JvmJobInfoExtractor]
      val pyExtractor = mock[PythonJobInfoExtractor]

      when(pyExtractor.extractInfo(any[File], any[String]))
        .thenReturn(Succ(JobInfo(data = ExtractedData("test"))))

      val baseJobInfoExtractor = new BaseJobInfoExtractor(jvmExtractor, pyExtractor)

      val info = baseJobInfoExtractor.extractInfo(new File("doesnt_matter.py"), "Test")

      info.get shouldBe JobInfo(data = ExtractedData("test"))

      verify(jvmExtractor, never()).extractInfo(any[File], any[String])
      verify(pyExtractor, times(1)).extractInfo(any[File], any[String])

    }
  }

  describe("JvmJobInfoExtractor") {


    it("should create JvmJobExtractor") {
      JvmJobInfoExtractor()
    }

    it("should extract job info prior to language") {
      val jobsLoader = mock[JobsLoader]
      val scalaJobInstance = mock[ScalaJobInstance]
      val javaJobInstance = mock[JavaJobInstance]
      val oldJobInstance = mock[OldInstanceWrapper]

      val jvmJobInfoExtractor = new JvmJobInfoExtractor(_ => jobsLoader)
      when(jobsLoader.loadJobInstance(mockitoEndsWith("Scala"), any[Action]))
        .thenReturn(Succ(scalaJobInstance))

      when(jobsLoader.loadJobInstance(mockitoEndsWith("Java"), any[Action]))
        .thenReturn(Succ(javaJobInstance))

      when(jobsLoader.loadJobInstance(mockitoEndsWith("Old"), any[Action]))
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
      res.get.data shouldBe ExtractedData(
        lang = "java",
        execute=Seq(UserInputArgument("num", MInt))
      )
      val scalaJob = jvmJobInfoExtractor.extractInfo(new File("doesnt_matter"), "TestScala")

      scalaJob.isSuccess shouldBe true
      scalaJob.get.data shouldBe ExtractedData(
        lang = "scala",
        execute=Seq(UserInputArgument("num", MInt))
      )
      val otherJob = jvmJobInfoExtractor.extractInfo(new File("doesnt_matter"), "TestOld")

      otherJob.isSuccess shouldBe true
      otherJob.get.data shouldBe ExtractedData(
        lang = "scala",
        execute=Seq(UserInputArgument("num", MInt))
      )
    }

    it("should return failure then jobsloader fails load instance") {
      val jobsLoader = mock[JobsLoader]
      when(jobsLoader.loadJobInstance(any[String], any[Action]))
        .thenReturn(Err(new IllegalArgumentException("invalid")))
      val jvmJobInfoExtractor = new JvmJobInfoExtractor(_ => jobsLoader)

      val res = jvmJobInfoExtractor.extractInfo(new File("doesnt_matter"), "Rest")

      res.isFailure shouldBe true
    }

    it("should load job instance for old serve method") {
      val oldInstance = mock[OldInstanceWrapper]
      val jobsLoader = mock[JobsLoader]
      when(jobsLoader.loadJobInstance(any[String], mockitoEq(Action.Execute)))
        .thenReturn(Err(new IllegalArgumentException("invalid")))
      when(oldInstance.describe())
        .thenReturn(Seq(
          UserInputArgument("num", MInt),
          InternalArgument()
        ))
      when(jobsLoader.loadJobInstance(any[String], mockitoEq(Action.Serve)))
        .thenReturn(Succ(oldInstance))
      val jvmJobInfoExtractor = new JvmJobInfoExtractor(_ => jobsLoader)

      val res = jvmJobInfoExtractor.extractInfo(new File("doesnt_matter"), "TestClass")

      res.isSuccess shouldBe true
      res.get.data shouldBe ExtractedData(
        lang="scala",
        execute = Seq(UserInputArgument("num", MInt)),
        isServe = true
      )
    }

    it("should get tags from internal arguments"){
      val javaJobInstance = mock[JavaJobInstance]
      val jobsLoader = mock[JobsLoader]

      when(jobsLoader.loadJobInstance(mockitoEndsWith("Java"), any[Action]))
        .thenReturn(Succ(javaJobInstance))

      when(javaJobInstance.describe())
        .thenReturn(Seq(
          UserInputArgument("num", MInt),
          InternalArgument(Seq("testTag"))
        ))
      val jvmJobInfoExtractor = new JvmJobInfoExtractor(_ => jobsLoader)

      val res = jvmJobInfoExtractor.extractInfo(new File("doesnt_matter"), "TestJava")
      res.isSuccess shouldBe true
      res.get.data shouldBe ExtractedData(
        lang = "java",
        execute = Seq(UserInputArgument("num", MInt)),
        tags = Seq("testTag")
      )

    }

  }
  describe("PyJobInfoExtractor") {
    it("should create py job info extractor") {
      new PythonJobInfoExtractor
    }

    it("should extract py info") {
      val pythonJobInfoExtractor = new PythonJobInfoExtractor
      val res = pythonJobInfoExtractor.extractInfo(new File("doesnt_matter"), "Test")
      res.isSuccess shouldBe true
      res.get.data shouldBe ExtractedData(
        lang="python"
      )
      res.get.instance shouldBe JobInstance.NoOpInstance
    }
  }




}
