package  io.hydrosphere.mist

import akka.actor.{ActorSystem, Props}
import akka.pattern.ask
import akka.testkit.TestKit
import akka.util.Timeout
import io.hydrosphere.mist.Messages.{ListMessage, StopAllContexts, StringMessage}
import io.hydrosphere.mist.worker.CLINode
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, WordSpecLike}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.sys.process._

class CLITest extends WordSpecLike with BeforeAndAfterAll with Eventually {

  implicit val system = ActorSystem("mist", MistConfig.Akka.CLI.settings)
  lazy val cliActor = system.actorOf(Props[CLINode], name = Constants.CLI.cliActorName)

  implicit val timeout = Timeout(5 seconds)

  object StartMist {
    val threadMaster = {
      new Thread {
        override def run() = {
          s"./bin/mist start master --config ${TestConfig.cliConfig}" !
        }
      }
    }
  }

  class StartJob(route: String, externalId: String) {
    val threadMaster = {
      new Thread {
        override def run() = {
          s"bin/mist start job --config ${TestConfig.cliConfig} --route ${route} --external-id ${externalId}".!
        }
      }.start()
    }
  }

  object StartJobs {
    val routers = List(("streaming-1", "job1"), ("streaming-2", "job2"), ("streaming-3", "job3"))
    def start() = {
      routers.foreach {
        router =>
          new StartJob(router._1, router._2)
          Thread.sleep(5000)
      }
    }
  }

  override def afterAll(): Unit = {

    "./bin/mist stop".!

    TestKit.shutdownActorSystem(system)

    StartMist.threadMaster.join()
  }

  override def beforeAll(): Unit = {
    StartMist.threadMaster.start()
  }

  "CLI Workers" must {
    "list no workers" in {
      eventually(timeout(30 seconds), interval(10 seconds)) {
        val future = cliActor ? new ListMessage(Constants.CLI.listWorkersMsg)
        val result = Await.result(future, timeout.duration).asInstanceOf[String]
        assert(Constants.CLI.noWorkersMsg == result)
      }
    }

    "start streaming and list workers" in {
      StartJobs.start()
      eventually(timeout(30 seconds), interval(10 seconds)) {
        val futureThreWorkers = cliActor ? new ListMessage(Constants.CLI.listWorkersMsg)
        val resultThreWorkers = Await.result(futureThreWorkers, timeout.duration).asInstanceOf[String]
        assert(Constants.CLI.noWorkersMsg != resultThreWorkers
          && resultThreWorkers.contains("streaming1")
          && resultThreWorkers.contains("streaming2")
          && resultThreWorkers.contains("streaming3"))
      }
    }

    "list thre jobs" in {
      eventually(timeout(30 seconds), interval(10 seconds)) {
        val futureTwoJobs = cliActor ? new ListMessage(Constants.CLI.listJobsMsg)
        val resultTwoJobs = Await.result(futureTwoJobs, timeout.duration).asInstanceOf[String]
        assert(Constants.CLI.noWorkersMsg != resultTwoJobs
          && resultTwoJobs.contains("job1")
          && resultTwoJobs.contains("job2")
          && resultTwoJobs.contains("job3"))
      }
    }

    "list two workers after kill first" in {
      cliActor ! new StringMessage(s"${Constants.CLI.stopWorkerMsg} streaming1")

      eventually(timeout(30 seconds), interval(10 seconds)) {
        val futureTwoWorkers = cliActor ? new ListMessage(Constants.CLI.listWorkersMsg)
        val resultTwoWorkers = Await.result(futureTwoWorkers, timeout.duration).asInstanceOf[String]
        assert( Constants.CLI.noWorkersMsg != resultTwoWorkers
          && !resultTwoWorkers.contains("streaming1")
          && resultTwoWorkers.contains("streaming2")
          && resultTwoWorkers.contains("streaming3"))
      }
    }

    "list two jobs" in {
      eventually(timeout(30 seconds), interval(10 seconds)) {
        val futureTwoJobs = cliActor ? new ListMessage(Constants.CLI.listJobsMsg)
        val resultTwoJobs = Await.result(futureTwoJobs, timeout.duration).asInstanceOf[String]
        assert(Constants.CLI.noWorkersMsg != resultTwoJobs
          && !resultTwoJobs.contains("job1")
          && resultTwoJobs.contains("job2")
          && resultTwoJobs.contains("job3"))
      }
    }

    "list two workers and one job after kill job" in {
      cliActor ! new StringMessage(s"${Constants.CLI.stopJobMsg} job2")

      eventually(timeout(30 seconds), interval(10 seconds)) {
        val futureOneJob = cliActor ? new ListMessage(Constants.CLI.listJobsMsg)
        val resultOneJob = Await.result(futureOneJob, timeout.duration).asInstanceOf[String]
        assert(!resultOneJob.contains("job2")
          && resultOneJob.contains("job3")
        )
      }
      eventually(timeout(30 seconds), interval(10 seconds)) {
        val futureTwoWorkers = cliActor ? new ListMessage(Constants.CLI.listWorkersMsg)
        val resultTwoWorkers = Await.result(futureTwoWorkers, timeout.duration).asInstanceOf[String]
        assert(!resultTwoWorkers.contains(Constants.CLI.noWorkersMsg)
          && resultTwoWorkers.contains("streaming2")
          && resultTwoWorkers.contains("streaming3"))
      }
    }

    "list no workers after kill all" in {
      val futureTwoWorkers = cliActor ? new ListMessage(Constants.CLI.listWorkersMsg)
      val resultTwoWorkers = Await.result(futureTwoWorkers, timeout.duration).asInstanceOf[String]
      assert(Constants.CLI.noWorkersMsg  != resultTwoWorkers)

      cliActor ! StopAllContexts
      eventually(timeout(30 seconds), interval(10 seconds)) {
        val futureNoWorkers = cliActor ? new ListMessage(Constants.CLI.listWorkersMsg)
        val resultNoWorkers = Await.result(futureNoWorkers, timeout.duration).asInstanceOf[String]
        assert(Constants.CLI.noWorkersMsg == resultNoWorkers)
      }
    }
  }
}
