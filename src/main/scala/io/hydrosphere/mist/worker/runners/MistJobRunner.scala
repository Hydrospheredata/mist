package io.hydrosphere.mist.worker.runners

import java.io.{ByteArrayInputStream, File}
import java.nio.file.{Files, Paths, StandardCopyOption}

import io.hydrosphere.mist.Messages.JobMessages.RunJobRequest
import io.hydrosphere.mist.jobs.resolvers.{JobResolver, LocalResolver}
import io.hydrosphere.mist.worker.NamedContext
import io.hydrosphere.mist.worker.runners.python.PythonRunner
import io.hydrosphere.mist.worker.runners.scala.ScalaRunner
import org.apache.commons.io.FilenameUtils

import _root_.scala.util.{Failure, Success, Try}
import scalaj.http.Http

class MistJobRunner(
  masterHttpHost: String,
  masterHttpPort: Int,
  jobRunnerSelector: File => JobRunner,
  savePath: String
) extends JobRunner {

  override def run(req: RunJobRequest, context: NamedContext): Either[String, Map[String, Any]] = {
    val filePath = req.params.filePath
    val fileT = JobResolver.fromPath(filePath, savePath) match {
      case r: LocalResolver if !r.exists => loadFromMaster(FilenameUtils.getName(filePath))
      case r => Try { r.resolve() }
    }
    fileT match {
      case Success(file) =>
        val specificRunner = jobRunnerSelector(file)
        specificRunner.run(req, context)
      case Failure(ex) =>
        Left(ex.getMessage)
    }
  }

  def loadFromMaster(filename: String): Try[File] = {
    val artifactUrl = s"http://$masterHttpHost:$masterHttpPort/api/v2/artifacts/"
    val millis = 120 * 1000 // 120 seconds
    val req = Http(artifactUrl + filename)
      .timeout(millis, millis)
    Try {
      val resp = req.asBytes

      if (resp.code == 200) {
        val filePath = Paths.get(savePath, filename)
        Files.copy(new ByteArrayInputStream(resp.body), filePath, StandardCopyOption.REPLACE_EXISTING)
        filePath.toFile
      }
      else
        throw new RuntimeException(s"failed to load from master: body ${resp.body}")
    }
  }
}


object MistJobRunner {

  val ExtensionMatchingRunnerSelector: File => JobRunner = {
    case f if f.getAbsolutePath.endsWith(".py") => new PythonRunner(f)
    case f if f.getAbsolutePath.endsWith(".jar") => new ScalaRunner(f)
    case x => throw new IllegalArgumentException(s"Can not select runner for ${x.toString}")
  }

  def apply(
    masterHttpHost: String,
    masterHttpPort: Int,
    savePath: String
  ): MistJobRunner =
    create(masterHttpHost, masterHttpPort, ExtensionMatchingRunnerSelector, savePath: String)

  def create(
    masterHttpHost: String,
    masterHttpPort: Int,
    jobRunnerSelector: File => JobRunner,
    savePath: String
  ): MistJobRunner = new MistJobRunner(masterHttpHost, masterHttpPort, jobRunnerSelector, savePath)
}

