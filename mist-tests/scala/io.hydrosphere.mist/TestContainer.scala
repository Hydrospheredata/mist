package io.hydrosphere.mist

import java.net.Socket

import com.github.dockerjava.api.model._
import com.github.dockerjava.core.command.PullImageResultCallback
import com.github.dockerjava.core.{DefaultDockerClientConfig, DockerClientBuilder}

import scala.annotation.tailrec
import scala.util._


class TestContainer(id: String, closeF: => Unit) {

  def close(): Unit = closeF

}

case class DockerImage(
  repo: Option[String],
  name: String,
  version: String
) {
  override def toString: String = repo match {
    case Some(r) => s"$r/$name:$version"
    case None => s"$name:$version"
  }
}

object DockerImage {
  def apply(repo: String, name: String, version: String): DockerImage =
    DockerImage(Some(repo), name, version)
  
  def apply(name: String, version: String): DockerImage = new DockerImage(None, name, version)
  
  val regex = "([A-Za-z0-9-]+)/([A-Za-z0-9-]+):([A-Za-z0-9-\\.]+)".r
  def apply(s: String): DockerImage = {
    s match {
      case regex(repo, name, version) => DockerImage(repo, name, version)
      case _ => throw new IllegalArgumentException(s"Invalid image $s")
    }
  }
}

object TestContainer {

  import scala.collection.JavaConverters._

  private val client = {
    val config = DefaultDockerClientConfig.createDefaultConfigBuilder()
      .withDockerHost("unix:///var/run/docker.sock")
      .build()

    DockerClientBuilder.getInstance(config).build()
  }

  def run(image: DockerImage, forward: Map[Int, Int], volumes: Map[String, String] = Map.empty): TestContainer = {

    def hasImage(image: DockerImage): Boolean = {
      import image._
      val filter = image.toString
      val images = client.listImagesCmd().withImageNameFilter(filter).exec()
      images.asScala.exists(i => i.getRepoTags.contains(image.toString))
    }

    def pullImage(image: DockerImage): Unit = {
      println(s"Pulling image: $image")
      client.pullImageCmd(image.toString).exec(new PullImageResultCallback).awaitSuccess()
    }

    def startFromImage(
      image: DockerImage,
      forward: Map[Int, Int],
      volumes: Map[String, String]
    ): String = {
      val ports = forward.map({case (k,v) => PortBinding.parse(s"$k:$v")})

      val binds = volumes.map({case (k,v) => Bind.parse(s"$k:$v")})
      val createRsp = client.createContainerCmd(image.toString)
          .withPortBindings(ports.toList.asJava)
          .withBinds(binds.toList.asJava)
          .exec()

      val id = createRsp.getId
      client.startContainerCmd(id).exec()

      if (forward.nonEmpty) {
        val port = forward.head._2
        val ip = client.inspectContainerCmd(id).exec().getNetworkSettings.getIpAddress
        awaitListening(ip, port, 20)
      }
      println(s"Container $id started from $image")
      id
    }

    @tailrec
    def awaitListening(addr: String, port: Int, max: Int, tried: Int = 0): Unit = {
      Try(new Socket(addr, port).close()) match {
        case Success(_) => ()
        case Failure(e) =>
          Thread.sleep(1000)
          if (tried < max)
            awaitListening(addr, port, max, tried + 1)
          else
            throw new RuntimeException(s"Container isn't listening on $addr:$port")
      }
    }

    def stopContainer(id: String): Unit = {
      client.stopContainerCmd(id).exec()
      println(s"Container $id stopped")
    }

    if (!hasImage(image)) pullImage(image)
    val id = startFromImage(image, forward, volumes)
    new TestContainer(id, stopContainer(id))
  }

}

