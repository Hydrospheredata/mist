package io.hydrosphere.mist.worker.runners

import java.io.{ByteArrayInputStream, File}
import java.nio.file.{Files, Paths}

import io.hydrosphere.mist.Messages.JobMessages.RunJobRequest
import io.hydrosphere.mist.jobs.resolvers.{JobResolver, LocalResolver}
import io.hydrosphere.mist.worker.NamedContext
import io.hydrosphere.mist.worker.runners.python.PythonRunner
import io.hydrosphere.mist.worker.runners.scala.ScalaRunner
import org.apache.commons.io.FilenameUtils

import _root_.scala.util.{Failure, Success, Try}
import scalaj.http.Http

class MistJobRunner(masterHttpHost: String, masterHttpPort: Int) extends JobRunner {

  override def run(req: RunJobRequest, context: NamedContext): Either[String, Map[String, Any]] = {
    val filePath = req.params.filePath
    val fileT = JobResolver.fromPath(filePath) match {
      case r: LocalResolver if !r.exists => loadFromMaster(FilenameUtils.getName(filePath))
      case r => Try { r.resolve() }
    }
    fileT match {
      case Success(file) =>
        val specificRunner = selectRunner(file)
        specificRunner.run(req, context)
      case Failure(ex) =>
        Left(ex.getMessage)
    }
  }

  private def loadFromMaster(filename: String): Try[File] = {
    val jobUrl = s"http://$masterHttpHost:$masterHttpPort/api/v2/artifacts/"
    val millis = 120 * 1000 // 120 seconds
    val req = Http(jobUrl + filename)
      .timeout(millis, millis)
    Try {
      val resp = req.asBytes

      if (resp.code == 200) {
        val filePath = Paths.get("/tmp", filename)
        Files.copy(new ByteArrayInputStream(resp.body), filePath)
        filePath.toFile
      }
      else
        throw new RuntimeException(s"Job failed body ${resp.body}")
    }
  }

  private def selectRunner(file: File): JobRunner = {
    val filePath = file.getAbsolutePath
    if (filePath.endsWith(".py"))
      new PythonRunner(file)
    else if (filePath.endsWith(".jar"))
      new ScalaRunner(file)
    else
      throw new IllegalArgumentException(s"Can not select runner for $filePath")
  }
}
