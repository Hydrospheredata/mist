package io.hydrosphere.mist.master.artifact

import java.io.File
import java.nio.file.{Files, Paths}
import java.util.concurrent.Executors

import io.hydrosphere.mist.master.data
import io.hydrosphere.mist.master.models.EndpointConfig

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Try}


trait ArtifactRepository {
  def listPaths(): Future[Set[String]]

  def get(filename: String): Option[File]

  def store(src: File, fileName: String): Future[File]
}

class FsArtifactRepository(
  rootDir: String)(implicit val ec: ExecutionContext)
  extends ArtifactRepository {

  override def listPaths(): Future[Set[String]] = {
    val artifactDir = Paths.get(rootDir).toFile
    if (artifactDir.isDirectory) {
      Future.successful(
        artifactDir.listFiles()
          .map(_.getName)
          .filter(allowedExtension)
          .toSet
      )
    } else Future.failed(new IllegalStateException(s"$rootDir is not directory"))
  }
  private def allowedExtension(fileName: String): Boolean = {
    fileName.endsWith(".jar") || fileName.endsWith(".py")
  }
  override def get(key: String): Option[File] = {
    Paths.get(rootDir, key).toFile match {
      case f if f.exists => Some(f)
      case f if !f.exists => None
    }
  }

  override def store(src: File, fileName: String): Future[File] = {
    Future {
      val dst = Paths.get(rootDir, fileName)
      Files.copy(src.toPath, dst)
      dst.toFile
    }
  }

}

class DefaultArtifactRepository(val default: Map[String, File])(implicit val ec: ExecutionContext)
  extends ArtifactRepository {

  override def listPaths(): Future[Set[String]] = Future.successful(default.keys.toSet)

  override def get(key: String): Option[File] = default.get(key)

  override def store(src: File, fileName: String): Future[File] =
    Future.failed(new UnsupportedOperationException("do not implement this"))
}

class SimpleArtifactRepository(
  mainRepo: ArtifactRepository,
  fallbackRepo: ArtifactRepository)(implicit ec: ExecutionContext)
  extends ArtifactRepository {

  override def listPaths(): Future[Set[String]] = for {
    mainRepoFiles <- mainRepo.listPaths()
    fallbackRepoFiles <- fallbackRepo.listPaths()
  } yield mainRepoFiles ++ fallbackRepoFiles

  override def get(filename: String): Option[File] =
    mainRepo.get(filename) orElse fallbackRepo.get(filename)

  override def store(src: File, fileName: String): Future[File] = {
    mainRepo.store(src, fileName)
  }
}

object ArtifactRepository {

  def create(
    storagePath: String,
    defaultEndpoints: Seq[EndpointConfig],
    jobsSavePath: String
  ): ArtifactRepository = {
    val ec = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(2))
    val toFile = fromEndpointConfig(_: EndpointConfig, jobsSavePath)
    // TODO there is enough one map and collect
    val defaultJobsPath = defaultEndpoints
      .map(e => e.path -> toFile(e))
      .toMap
      .collect { case (key, Success(file)) => key -> file }

    val defaultArtifactRepo = new DefaultArtifactRepository(defaultJobsPath)(ec)
    val fsArtifactRepo = new FsArtifactRepository(data.checkDirectory(storagePath).toString)(ec)
    new SimpleArtifactRepository(fsArtifactRepo, defaultArtifactRepo)(ec)
  }

  private def fromEndpointConfig(e: EndpointConfig, savePath: String): Try[File] =
    Try { JobResolver.fromPath(e.path, savePath).resolve() }

}