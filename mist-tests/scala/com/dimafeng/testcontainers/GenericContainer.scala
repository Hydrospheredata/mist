package com.dimafeng.testcontainers

import org.testcontainers.containers.wait.WaitStrategy
import org.testcontainers.containers.{BindMode, FixedHostPortGenericContainer => OTCGenericContainer}
import org.testcontainers.shaded.com.github.dockerjava.api.model.VolumesFrom

class GenericContainer(imageName: String,
  fixedPorts: Map[Int, Int] = Map.empty,
  exposedPorts: Seq[Int] = Seq(),
  env: Map[String, String] = Map(),
  command: Seq[String] = Seq(),
  classpathResourceMapping: Seq[(String, String, BindMode)] = Seq(),
  waitStrategy: Option[WaitStrategy] = None,
  volumes: Seq[(String, String, BindMode)] = Seq.empty
) extends SingleContainer[OTCGenericContainer[_]] {

  type OTCContainer = OTCGenericContainer[T] forSome {type T <: OTCGenericContainer[T]}
  override implicit val container: OTCContainer = new OTCGenericContainer(imageName)

  if (exposedPorts.nonEmpty) {
    container.withExposedPorts(exposedPorts.map(int2Integer): _*)
  }
  if(fixedPorts.nonEmpty) {
    fixedPorts.foreach({case (from, to) => container.withFixedExposedPort(from, to)})
  }
  env.foreach(Function.tupled(container.withEnv))
  if (command.nonEmpty) {
    container.withCommand(command: _*)
  }
  volumes.foreach(Function.tupled(container.addFileSystemBind))
  classpathResourceMapping.foreach(Function.tupled(container.withClasspathResourceMapping))
  waitStrategy.foreach(container.waitingFor)
}

object GenericContainer {
  def apply(imageName: String,
    fixedExposedPorts: Map[Int, Int] = Map.empty,
    exposedPorts: Seq[Int] = Seq(),
    env: Map[String, String] = Map(),
    command: Seq[String] = Seq(),
    classpathResourceMapping: Seq[(String, String, BindMode)] = Seq(),
    waitStrategy: WaitStrategy = null,
    volumes: Seq[(String, String, BindMode)] = Seq.empty
  ) =
    new GenericContainer(
      imageName,
      fixedExposedPorts,
      exposedPorts,
      env,
      command,
      classpathResourceMapping,
      Option(waitStrategy),
      volumes
    )
}

