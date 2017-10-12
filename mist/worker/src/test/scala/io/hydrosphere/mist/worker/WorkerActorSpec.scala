package io.hydrosphere.mist.worker

import java.io.File

import mist.api.data._

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.testkit.{TestActorRef, TestKit, TestProbe}
import io.hydrosphere.mist.core.CommonData._
import io.hydrosphere.mist.core.MockitoSugar
import io.hydrosphere.mist.worker.runners.{ArtifactDownloader, JobRunner, RunnerSelector}
import mist.api.data.JsLikeData
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._
import org.scalatest.prop.TableDrivenPropertyChecks._

import scala.concurrent.duration._

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

    val workers = Table[String, WorkerProps](
      ("name", "f"),
      ("shared", (r: RunnerSelector) => SharedWorkerActor.props(r, context, artifactDownloader, Duration.Inf, 10)),
      ("exclusive", (r: RunnerSelector) => ExclusiveWorkerActor.props(r, context, artifactDownloader))
    )

    forAll(workers) { (name, makeProps) =>
      it(s"should execute jobs in $name mode") {
        val runner = SuccessRunnerSelector(JsLikeInt(42))
        val worker = createActor(makeProps(runner))

        val probe = TestProbe()

        probe.send(worker, RunJobRequest("id", JobParams("path", "MyClass", Map.empty, action = Action.Execute)))

        probe.expectMsgType[JobFileDownloading]
        probe.expectMsgType[JobStarted]
        probe.expectMsgPF(){
          case JobSuccess("id", r) =>
            r shouldBe JsLikeInt(42)
        }
      }

      it(s"should respond failure in $name mode") {
        val runner = FailureRunnerSelector("Expected error")
        val worker = createActor(makeProps(runner))

        val probe = TestProbe()
        probe.send(worker, RunJobRequest("id", JobParams("path", "MyClass", Map.empty, action = Action.Execute)))
        probe.expectMsgType[JobFileDownloading]
        probe.expectMsgType[JobStarted]
        probe.expectMsgPF() {
          case JobFailure("id", e) =>
            e should not be empty
        }
      }

      it(s"should cancel job in $name mode") {
        val runnerSelector = RunnerSelector(new JobRunner {
          override def run(req: RunJobRequest, c: NamedContext): Either[Throwable, JsLikeData] = {
            val sc = c.sparkContext
            val r = sc.parallelize(1 to 10000, 2).map { i => Thread.sleep(10000); i }.count()
            Right(JsLikeMap("r" -> JsLikeString("Ok")))
          }
        })

        val worker = createActor(makeProps(runnerSelector))

        val probe = TestProbe()
        probe.send(worker, RunJobRequest("id", JobParams("path", "MyClass", Map.empty, action = Action.Execute)))
        probe.send(worker, CancelJobRequest("id"))

        probe.expectMsgAllConformingOf(classOf[JobFileDownloading], classOf[JobStarted], classOf[JobIsCancelled])
      }
      def createActor(props: Props): ActorRef = {
        TestActorRef[Actor](props)
      }

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

    val props = SharedWorkerActor.props(runnerSelector, context, artifactDownloader, Duration.Inf, 2)
    val worker = TestActorRef[SharedWorkerActor](props)

    probe.send(worker, RunJobRequest("1", JobParams("path", "MyClass", Map.empty, action = Action.Execute)))
    probe.send(worker, RunJobRequest("2", JobParams("path", "MyClass", Map.empty, action = Action.Execute)))
    probe.send(worker, RunJobRequest("3", JobParams("path", "MyClass", Map.empty, action = Action.Execute)))

    probe.expectMsgAllConformingOf(
      classOf[JobFileDownloading],
      classOf[JobFileDownloading],
      classOf[JobStarted],
      classOf[JobStarted],
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

