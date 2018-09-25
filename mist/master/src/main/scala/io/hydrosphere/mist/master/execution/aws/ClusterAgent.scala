package io.hydrosphere.mist.master.execution.aws

import akka.actor.{ActorRef, ActorSystem}
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.hydrosphere.mist.common.CommonData
import io.hydrosphere.mist.utils.NetUtils
import io.hydrosphere.mist.utils.akka.{ActorRegHub, WhenTerminated}

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object ClusterAgent {

  def main(args: Array[String]): Unit = {
    val masterAddr = args(0)
    val id = args(1)
    val accessKey = args(2)
    val secretKey = args(3)
    val region = args(4)
    val awsId = args(5)

    val emrClient = EMRClient.create(accessKey, secretKey, region)

    val hostname = NetUtils.findLocalInetAddress()

    val config = ConfigFactory.load("agent")
      .withValue("akka.remote.netty.tcp.hostname", ConfigValueFactory.fromAnyRef(hostname))

    implicit val system = ActorSystem("mist-info-provider", config)

    def resolveRemote(path: String): ActorRef = {
      val ref = system.actorSelection(path).resolveOne(10 seconds)
      try {
        Await.result(ref, Duration.Inf)
      } catch {
        case e: Throwable =>
          println(s"Couldn't resolve remote path $path")
          e.printStackTrace()
          sys.exit(-1)
      }
    }

    def remotePath(addr: String, name: String): String = {
      s"akka.tcp://mist@$addr/user/$name"
    }

    val regHub = resolveRemote(remotePath(masterAddr, "regHub"))
    val heathRef = resolveRemote(remotePath(masterAddr, CommonData.HealthActorName))
    regHub ! ActorRegHub.Register(id)

    WhenTerminated(heathRef, {
      println("Remote system was terminated, shutdown cluster")
      emrClient.stop(awsId).unsafeRunSync()
      system.terminate()
    })

  }
}
