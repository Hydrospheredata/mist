package io.hydrosphere.mist.master.artifact

import java.io.File
import java.nio.file.Paths
import java.util.concurrent.Executors

import io.hydrosphere.mist.master.data.EndpointsStorage
import org.apache.commons.io.{FileUtils, FilenameUtils}

import scala.concurrent.{ExecutionContext, Future}


trait ArtifactRepository {
  def listPaths(): Future[Set[String]]

  def get(filename: String): Future[Option[File]]

  def store(src: File, fileName: String): Future[File]
}

class FsArtifactRepository(
  rootDir: String)(implicit val ec: ExecutionContext)
  extends ArtifactRepository {

  override def listPaths(): Future[Set[String]] = {
    val artifactDir = Paths.get(rootDir).toFile
    if (artifactDir.isDirectory) {
      Future.successful(artifactDir.listFiles().map(_.getName).toSet)
    } else Future.failed(new IllegalStateException(s"$rootDir is not directory"))
  }

  override def get(filename: String): Future[Option[File]] = {
    Future.successful(
      Paths.get(rootDir, filename).toFile match {
        case f if f.exists => Some(f)
        case f if !f.exists => None
      }
    )
  }

  override def store(src: File, fileName: String): Future[File] = {
    Future {
      val dst = Paths.get(rootDir, fileName).toFile
      FileUtils.copyFile(src, dst)
      dst
    }
  }

}

class DefaultArtifactRepository(
  endpointsStorage: EndpointsStorage)(implicit val ec: ExecutionContext)
  extends ArtifactRepository {

  override def listPaths(): Future[Set[String]] = endpointsStorage.all.map(files => {
    files.map(_.path)
      .map {
        case p if p.startsWith("hdfs") || p.startsWith("mvn") => p
        case p => FilenameUtils.getName(p)
      }
      .toSet
  })

  override def get(filename: String): Future[Option[File]] = {
    endpointsStorage.all.map(configs => {
      configs.map(_.path)
        .map(str => (str, str.substring(str.lastIndexOf('/') + 1, str.length)))
        .find {
          _._2 == filename
        }
        .map(_._1)
        .map(new File(_))
    })
  }

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

  override def get(filename: String): Future[Option[File]] = {
    for {
      f <- mainRepo.get(filename)
      fallback <- fallbackRepo.get(filename)
    } yield f orElse fallback
  }

  override def store(src: File, fileName: String): Future[File] = {
    mainRepo.store(src, fileName)
  }
}

object ArtifactRepository {

  def create(storagePath: String, endpointsStorage: EndpointsStorage): ArtifactRepository = {
    val ec = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(2))
    val defaultArtifactRepo = new DefaultArtifactRepository(endpointsStorage)(ec)
    val fsArtifactRepo = new FsArtifactRepository(storagePath)(ec)
    new SimpleArtifactRepository(fsArtifactRepo, defaultArtifactRepo)(ec)
  }
}