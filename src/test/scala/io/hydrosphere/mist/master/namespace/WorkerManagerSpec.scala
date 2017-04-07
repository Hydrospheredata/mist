package io.hydrosphere.mist.master.namespace

import akka.actor.Actor.Receive
import akka.actor.{Address, Actor, Props, ActorSystem}
import akka.cluster.Cluster
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import io.hydrosphere.mist.jobs.Action
import io.hydrosphere.mist.master.namespace.ClusterWorker.WorkerRegistration
import io.hydrosphere.mist.master.namespace.WorkerActor.{JobParams, JobSuccess, JobStarted, RunJobRequest}
import io.hydrosphere.mist.master.namespace.WorkersManager.WorkerCommand
import org.scalatest.{FunSpecLike, FunSpec}

import WorkerManagerSpec._

import scala.concurrent.Promise

class WorkerManagerSpec extends TestKit(ActorSystem(systemName, config))
  with ImplicitSender
  with FunSpecLike {

  val NothingRunner = new WorkerRunner {
    override def run(settings: WorkerSettings): Unit = {}
  }


  it("should yoyo") {
    val frontendFactory = (name: String) => {
      system.actorOf(Props(classOf[FrontendFixture]))
    }

    val manager = system.actorOf(
      Props(classOf[WorkersManager],
        frontendFactory,
        NothingRunner))

    val params = JobParams("path", "MyClass", Map.empty, Action.Execute)
    manager ! WorkerCommand("test", RunJobRequest("id", params))

    expectMsgType[ExecutionInfo]
  }

  class FrontendFixture extends Actor {
    override def receive: Actor.Receive = {
      case r @ RunJobRequest(id, params) =>
        val result = Promise[Map[String, Any]]
        result.success(Map("r" -> "ok"))

        val jobDone = ExecutionInfo(r, result)
        //sender() ! jobDone
    }
  }
}

object WorkerManagerSpec {

  val systemName = "TestWorkersManager"

  val config = ConfigFactory.parseString(
    s"""
       |akka {
       |  loggers = ["akka.event.slf4j.Slf4jLogger"]
       |  logging-filter = "akka.event.slf4j.Slf4jLoggingFilter"
       |  actor.provider = "akka.cluster.ClusterActorRefProvider"
       |
       |  remote {
       |    log-remote-lifecycle-events = off
       |    log-recieved-messages = off
       |    netty.tcp {
       |      hostname = "127.0.0.1"
       |      port = 2551
       |    }
       |    transport-failure-detector {
       |      heartbeat-interval = 30s
       |      acceptable-heartbeat-pause = 5s
       |    }
       |  }
       |
       |  cluster {
       |    seed-nodes = ["akka.tcp://$systemName@127.0.0.1:2551"]
       |    auto-down-unreachable-after = 10s
       |  }
       |}
     """.stripMargin

  )
}
