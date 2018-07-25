package io.hydrosphere.mist.master.artifact

import java.io.File
import java.nio.file.{Files, Paths, StandardCopyOption}
import java.util.concurrent.Executors

import io.hydrosphere.mist.master.data
import io.hydrosphere.mist.master.models.FunctionConfig

import scala.concurrent.{ExecutionContext, Future}


trait ArtifactRepository {
  def listPaths(): Future[Set[String]]

  def get(filename: String): Option[File]

  def store(src: File, fileName: String): Future[File]

  def delete(filename: String): Option[String]
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
          .toSet
      )
    } else Future.failed(new IllegalStateException(s"$rootDir is not directory"))
  }

  override def get(key: String): Option[File] = {
    Paths.get(rootDir, key).toFile match {
      case f if f.exists => Some(f)
      case f if !f.exists => None
    }
  }

  override def delete(filename: String): Option[String] =
    get(filename).map(f => {
      f.delete()
      filename
    })

  override def store(src: File, fileName: String): Future[File] = {
    Future {
      val dst = Paths.get(rootDir, fileName)
      Files.copy(src.toPath, dst, StandardCopyOption.REPLACE_EXISTING)
      dst.toFile
    }
  }

}

class DefaultArtifactRepository(val default: Map[String, File])(implicit val ec: ExecutionContext)
  extends ArtifactRepository {

  override def listPaths(): Future[Set[String]] = Future.successful(default.keys.toSet)

  override def get(key: String): Option[File] = {
    default.get(key).orElse({
      if (Files.exists(Paths.get(key))) Some(new File(key)) else None
    })
  }

  override def store(src: File, fileName: String): Future[File] =
    Future.failed(new UnsupportedOperationException("do not implement this"))

  override def delete(filename: String): Option[String] = None
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

  override def delete(filename: String): Option[String] = mainRepo.delete(filename)
}

object ArtifactRepository {

  def create(
    storagePath: String,
    defaultEndpoints: Seq[FunctionConfig]
  ): ArtifactRepository = {
    val ec = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(2))
    val defaultJobsPath = defaultEndpoints
      .map(e => e.path -> new File(e.path))
      .filter(_._2.exists())
      .toMap

    val defaultArtifactRepo = new DefaultArtifactRepository(defaultJobsPath)(ec)
    val fsArtifactRepo = new FsArtifactRepository(data.checkDirectory(storagePath).toString)(ec)
    new SimpleArtifactRepository(fsArtifactRepo, defaultArtifactRepo)(ec)
  }


}