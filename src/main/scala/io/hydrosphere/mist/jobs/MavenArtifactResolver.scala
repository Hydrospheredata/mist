package io.hydrosphere.mist.jobs

import java.io.File
import java.nio.file.{Files, Paths}

/**
  * Maven-like artifact resolver
  * Currently it can download only ONE root jar
  */
case class MavenArtifactResolver(
  repoUrl: String,
  artifact: MavenArtifact,
  targetDirectory: String = "/tmp"
) extends JobFile {

  import scalaj.http._

  override def exists: Boolean = {
    val resp = Http(jarUlr).method("HEAD").asString
    resp.code == 200
  }

  override def file: File = {
    val resp = Http(jarUlr).method("GET").asBytes
    if (resp.code != 200) {
      val message =
        s"""
           |Could not find $jarUlr.
           | Response code: ${resp.code} ${new String(resp.body)}
         """.stripMargin
      println(message)
      throw new JobFile.NotFoundException(message)
    } else {
      val localPath = Paths.get(targetDirectory, artifact.jarName)
      Files.write(localPath, resp.body)
      localPath.toFile
    }
  }

  private def jarUlr: String =
    s"$repoUrl/${artifact.jarPath}".replaceAll("(?<!http:|https:)//", "/")

}

object MavenArtifactResolver {
  import scala.util.matching.Regex

  /**
    * Pattern for catching job file definition ib routes conf
    * Example: "mvn://http://repo1.maven.org/maven2 :: org.company % name % version"
    */
  val pattern = "mvn://" +
    "(https?://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|])\\s?::\\s?" + // repo - host:port/path
    "(.+) % (.+) % (.+)"r

  def fromPath(p: String): MavenArtifactResolver = pattern.findFirstMatchIn(p) match {
    case Some(m) => toResolver(m)
    case None =>
      val message =
        s"""Invalid path $p for maven artifact resolver.
           |String should be like: \"mvn://host:port/myrepo:: org % name % version\" """.stripMargin
      throw new IllegalArgumentException(message)
  }

  private def toResolver(m: Regex.Match): MavenArtifactResolver = {
    val groups = m.subgroups

    val repoUrl = groups(0)
    val artifact = MavenArtifact(groups(1), groups(2), groups(3))
    new MavenArtifactResolver(repoUrl, artifact)
  }
}

case class MavenArtifact(
  organization: String,
  name: String,
  version: String
) {

  def jarPath: String = {
    val org = organization.split('.').toList
    val last = List(
      name,
      version,
      jarName
    )
    (org ++ last).mkString("/")
  }

  def jarName: String = s"$name-$version.jar"
}