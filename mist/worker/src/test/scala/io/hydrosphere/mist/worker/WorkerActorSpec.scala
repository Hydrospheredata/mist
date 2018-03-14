package io.hydrosphere.mist.worker

import java.io.File

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{TestActorRef, TestKit, TestProbe}
import io.hydrosphere.mist.core.CommonData._
import io.hydrosphere.mist.core.MockitoSugar
import io.hydrosphere.mist.worker.runners.{ArtifactDownloader, JobRunner, RunnerSelector}
import mist.api.data.{JsLikeData, _}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._

class WorkerActorSpec extends TestKit(ActorSystem("WorkerSpec"))
  with FunSpecLike
  with Matchers
  with MockitoSugar
  with BeforeAndAfterAll {

  val conf = new SparkConf()
    .setMaster("local[2]")
    .setAppName("test")
    .set("spark.driver.allowMultipleContexts", "true")

  var context: NamedContext = _
  var spContext: SparkContext = _

  override def afterAll {
    TestKit.shutdownActorSystem(system)
    context.stop()
    spContext.stop()
  }

  override def beforeAll {
    spContext = new SparkContext(conf)
    context = new NamedContext(spContext, "test") {
      override def stop(): Unit = {} //do not close ctx during tests
    }
  }

  describe("common behavior") {

    type WorkerProps = RunnerSelector => Props

    val artifactDownloader = mock[ArtifactDownloader]

    when(artifactDownloader.downloadArtifact(any[String]))
      .thenSuccess(new File("doesn't matter"))

    it(s"should execute jobs") {
      val runner = SuccessRunnerSelector(JsLikeNumber(42))
      val worker = createActor(runner)
      val probe = TestProbe()

      probe.send(worker, RunJobRequest("id", JobParams("path", "MyClass", Map.empty, action = Action.Execute)))

      probe.expectMsgType[JobFileDownloading]
      probe.expectMsgType[JobStarted]
      probe.expectMsgPF(){
        case JobSuccess("id", r) =>
          r shouldBe JsLikeNumber(42)
      }
    }

    it(s"should respond failure") {
      val runner = FailureRunnerSelector("Expected error")
      val worker = createActor(runner)

      val probe = TestProbe()
      probe.send(worker, RunJobRequest("id", JobParams("path", "MyClass", Map.empty, action = Action.Execute)))
      probe.expectMsgType[JobFileDownloading]
      probe.expectMsgType[JobStarted]
      probe.expectMsgPF() {
        case JobFailure("id", e) =>
          e should not be empty
      }
    }

    it(s"should cancel job") {
      val runnerSelector = RunnerSelector(new JobRunner {
        override def run(req: RunJobRequest, c: NamedContext): Either[Throwable, JsLikeData] = {
          val sc = c.sparkContext
          val r = sc.parallelize(1 to 10000, 2).map { i => Thread.sleep(10000); i }.count()
          Right(JsLikeMap("r" -> JsLikeString("Ok")))
        }
      })

      val worker = createActor(runnerSelector)

      val probe = TestProbe()
      probe.send(worker, RunJobRequest("id", JobParams("path", "MyClass", Map.empty, action = Action.Execute)))
      probe.send(worker, CancelJobRequest("id"))

      probe.expectMsgAllConformingOf(classOf[JobFileDownloading], classOf[JobStarted], classOf[JobIsCancelled])
    }
    def createActor(runnerSelector: RunnerSelector): ActorRef = {
      val props  = WorkerActor.props(context, artifactDownloader, runnerSelector)
      TestActorRef[WorkerActor](props)
    }

  }

  it("should limit jobs") {
    val runnerSelector = SuccessRunnerSelector({
      Thread.sleep(1000)
      JsLikeMap("yoyo" -> JsLikeString("hey"))
    })

    val probe = TestProbe()

    val artifactDownloader = mock[ArtifactDownloader]
    when(artifactDownloader.downloadArtifact(any[String]))
      .thenSuccess(new File("doesn't matter"))

    val props = WorkerActor.props(context, artifactDownloader, runnerSelector)
    val worker = TestActorRef[WorkerActor](props)

    probe.send(worker, RunJobRequest("1", JobParams("path", "MyClass", Map.empty, action = Action.Execute)))
    probe.send(worker, RunJobRequest("2", JobParams("path", "MyClass", Map.empty, action = Action.Execute)))
    probe.send(worker, RunJobRequest("3", JobParams("path", "MyClass", Map.empty, action = Action.Execute)))

    probe.expectMsgAllConformingOf(
      classOf[JobFileDownloading],
      classOf[JobStarted],
      classOf[WorkerIsBusy],
      classOf[WorkerIsBusy]
    )
  }

  def RunnerSelector(r: JobRunner): RunnerSelector =
    new RunnerSelector {
      override def selectRunner(file: File): JobRunner = r
    }

  def SuccessRunnerSelector(r: => JsLikeData): RunnerSelector =
    new RunnerSelector {
      override def selectRunner(file: File): JobRunner = SuccessRunner(r)
    }

  def FailureRunnerSelector(error: String): RunnerSelector =
    new RunnerSelector {
      override def selectRunner(file: File): JobRunner = FailureRunner(error)
    }

  def SuccessRunner(r: => JsLikeData): JobRunner =
    testRunner(Right(r))

  def FailureRunner(error: String): JobRunner =
    testRunner(Left(new RuntimeException(error)))

  def testRunner(f: => Either[Throwable, JsLikeData]): JobRunner = {
    new JobRunner {
      def run(p: RunJobRequest, c: NamedContext): Either[Throwable, JsLikeData] = f
    }
  }
}

