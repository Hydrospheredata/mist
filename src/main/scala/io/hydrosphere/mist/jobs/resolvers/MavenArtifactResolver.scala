package io.hydrosphere.mist.jobs.resolvers

import java.io.File
import java.nio.file.{Files, Paths}

import io.hydrosphere.mist.jobs.JobFile
import org.apache.commons.codec.digest.DigestUtils

/**
  * Maven-like artifact resolver
  * Currently it can download only ONE root jar
  */
case class MavenArtifactResolver(
  repoUrl: String,
  artifact: MavenArtifact,
  targetDirectory: String = "/tmp"
) extends JobResolver {

  import scalaj.http._

  override def exists: Boolean = {
    val resp = Http(jarUlr).method("HEAD").asString
    resp.code == 200
  }

  override def resolve(): File = {
    if (!exists) {
      throw new JobFile.NotFoundException(s"maven file not exists at $repoUrl")
    }
    val content = download(jarUlr)
    if (validateContent(content)) {
      val localPath = Paths.get(targetDirectory, artifact.jarName)
      Files.write(localPath, content)
      localPath.toFile
    } else {
      val message = s"Different checksums for downloaded $artifact"
      throw new JobFile.UnknownTypeException(message)
    }
  }

  private def validateContent(bytes: Array[Byte]): Boolean = {
    val data = download(checkSumUrl)
    DigestUtils.sha1Hex(bytes) == new String(data)
  }

  private def download(url: String): Array[Byte] = {
    val resp = Http(url).method("GET").asBytes
    if (resp.code != 200) {
      val message =
        s"""
           |Maven resolver request failed: $url.
           | Response code: ${resp.code} ${new String(resp.body)}
         """.stripMargin
      throw new JobFile.UnknownTypeException(message)
    } else {
      resp.body
    }
  }

  private def jarUlr: String =
    s"$repoUrl/${artifact.jarPath}".replaceAll("(?<!http:|https:)//", "/")

  private def checkSumUrl: String =
    jarUlr + ".sha1"

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