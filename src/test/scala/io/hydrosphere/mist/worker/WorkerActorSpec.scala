package io.hydrosphere.mist.worker

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.testkit.{TestActorRef, TestActor, TestKit, TestProbe}
import io.hydrosphere.mist.Messages.JobMessages._
import io.hydrosphere.mist.jobs.Action
import io.hydrosphere.mist.worker.runners.JobRunner
import org.apache.spark.SparkConf
import org.scalatest._
import org.scalatest.prop.TableDrivenPropertyChecks._

import scala.concurrent.duration.Duration

class WorkerActorSpec extends TestKit(ActorSystem("WorkerSpec"))
  with FunSpecLike
  with Matchers
  with BeforeAndAfterAll {

  val conf = new SparkConf()
    .setMaster("local[2]")
    .setAppName("test")
    .set("spark.driver.allowMultipleContexts", "true")

  var context: NamedContext = _

  override def afterAll {
    TestKit.shutdownActorSystem(system)
    context.stop()
  }

  override def beforeAll {
    context = NamedContext("test", conf)
  }

  describe("common behavior") {

    type WorkerProps = JobRunner => Props

    val workers = Table[String, WorkerProps](
      ("name", "f"),
      ("shared", (r: JobRunner) => WorkerActor.props(Shared(10, Duration.Inf), context, r)),
      ("exclusive", (r: JobRunner) => WorkerActor.props(Exclusive, context, r))
    )

    forAll(workers) { (name, makeProps) =>
      it(s"should execute jobs in $name mode") {
        val runner = SuccessRunner(Map("answer" -> 42))

        val probe = TestProbe()
        val worker = createActor(makeProps(runner))

        probe.send(worker, RunJobRequest("id", JobParams("path", "MyClass", Map.empty, action = Action.Execute)))

        probe.expectMsgType[JobStarted]
        probe.expectMsgPF(){
          case JobSuccess("id", r) =>
            r shouldBe Map("answer" -> 42)
        }
      }

      it(s"should respond failure in $name mode") {
        val runner = FailureRunner("Expected error")
        val worker = createActor(makeProps(runner))

        val probe = TestProbe()
        probe.send(worker, RunJobRequest("id", JobParams("path", "MyClass", Map.empty, action = Action.Execute)))

        probe.expectMsgType[JobStarted]
        probe.expectMsgPF(){
          case JobFailure("id", e) =>
            e shouldBe "Expected error"
        }
      }

      it(s"should cancel job in $name mode") {
        val runner = new JobRunner {
          override def run(p: JobParams, c: NamedContext): Either[String, Map[String, Any]] = {
            val sc = c.context
            val r = sc.parallelize(1 to 10000, 2).map { i => Thread.sleep(10000); i }.count()
            Right(Map("r" -> "Ok"))
          }
        }

        val worker = createActor(makeProps(runner))

        val probe = TestProbe()
        probe.send(worker, RunJobRequest("id", JobParams("path", "MyClass", Map.empty, action = Action.Execute)))
        probe.send(worker, CancelJobRequest("id"))

        probe.expectMsgType[JobStarted]
        probe.expectMsgType[JobIsCancelled]
      }

      def createActor(props: Props): ActorRef = {
        TestActorRef[Actor](props)
      }

    }
  }

  it("should limit jobs") {
    val runner = SuccessRunner({
      Thread.sleep(1000)
      Map("yoyo" -> "hey")
    })

    val probe = TestProbe()

    val props = WorkerActor.props(Shared(2, Duration.Inf), context, runner)
    val worker = TestActorRef[SharedWorkerActor](props)

    probe.send(worker, RunJobRequest("1", JobParams("path", "MyClass", Map.empty, action = Action.Execute)))
    probe.send(worker, RunJobRequest("2", JobParams("path", "MyClass", Map.empty, action = Action.Execute)))
    probe.send(worker, RunJobRequest("3", JobParams("path", "MyClass", Map.empty, action = Action.Execute)))

    probe.expectMsgType[JobStarted]
    probe.expectMsgType[JobStarted]
    probe.expectMsgType[WorkerIsBusy]
  }

  def SuccessRunner(r: => Map[String, Any]): JobRunner =
    testRunner(Right(r))

  def FailureRunner(error: String): JobRunner =
    testRunner(Left(error))

  def testRunner(f: => Either[String, Map[String, Any]]): JobRunner = {
    new JobRunner {
      def run(p: JobParams, c: NamedContext): Either[String, Map[String, Any]] = f
    }
  }
}

