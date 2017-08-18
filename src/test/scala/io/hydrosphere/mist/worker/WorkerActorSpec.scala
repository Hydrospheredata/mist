package io.hydrosphere.mist.worker

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.testkit.{TestActorRef, TestKit, TestProbe}
import io.hydrosphere.mist.Messages.JobMessages._
import io.hydrosphere.mist.jobs.Action
import io.hydrosphere.mist.worker.runners.JobRunner
import org.apache.spark.{SparkConf, SparkContext}
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
    context.sparkContext.stop()
  }

  override def beforeAll {
    val spContext = new SparkContext(conf)
    context = new NamedContext(spContext, "test") {
      override def stop(): Unit = {} //do not close ctx during tests
    }
  }

  describe("common behavior") {

    type WorkerProps = JobRunner => Props

    val workers = Table[String, WorkerProps](
      ("name", "f"),
      ("shared", (r: JobRunner) => SharedWorkerActor.props(context, r, Duration.Inf, 10)),
      ("exclusive", (r: JobRunner) => ExclusiveWorkerActor.props(context, r))
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
          override def run(req: RunJobRequest, c: NamedContext): Either[String, Map[String, Any]] = {
            val sc = c.sparkContext
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

    val props = SharedWorkerActor.props(context, runner, Duration.Inf, 2)
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
      def run(p: RunJobRequest, c: NamedContext): Either[String, Map[String, Any]] = f
    }
  }
}

