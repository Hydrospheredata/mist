package io.hydrosphere.mist.master.artifact

import java.io.File
import java.util.concurrent.Executors

import akka.stream.scaladsl.Source
import akka.util.ByteString
import io.hydrosphere.mist.master.data.EndpointsStorage

import scala.concurrent.{ExecutionContext, Future}


trait ArtifactRepository {
  def loadAll(): Future[Seq[File]]
  def get(filename: String): Future[Option[File]]
  def store(src: Source[ByteString, Any], fileName: String): Future[File]
}

class FsArtifactRepository(rootDir: String) extends ArtifactRepository {

  override def loadAll(): Future[Seq[File]] = ???

  override def get(filename: String): Future[Option[File]] = ???

  override def store(src: Source[ByteString, Any], fileName: String): Future[File] = ???

}

class DefaultArtifactRepository(endpointsStorage: EndpointsStorage) extends ArtifactRepository {

  override def loadAll(): Future[Seq[File]] = ???

  override def get(filename: String): Future[Option[File]] = ???

  override def store(src: Source[ByteString, Any], fileName: String): Future[File] = ???

}

class SimpleArtifactRepository(
  mainRepo: ArtifactRepository,
  fallbackRepo: ArtifactRepository)(implicit ec: ExecutionContext)
  extends ArtifactRepository {

  override def loadAll(): Future[Seq[File]] = {
    for {
      mainRepoFiles <- mainRepo.loadAll()
      fallbackRepoFiles <- fallbackRepo.loadAll()
    } yield mainRepoFiles ++ fallbackRepoFiles
  }

  override def get(filename: String): Future[Option[File]] = {
    for {
      f <- mainRepo.get(filename)
      fallback <- fallbackRepo.get(filename)
    } yield f orElse fallback
  }

  override def store(src: Source[ByteString, Any], fileName: String): Future[File] = {
    mainRepo.store(src, fileName)
  }
}

object ArtifactRepository {

  def create(storagePath: String, endpointsStorage: EndpointsStorage): ArtifactRepository = {
    val defaultArtifactRepo = new DefaultArtifactRepository(endpointsStorage)
    val fsArtifactRepo = new FsArtifactRepository(storagePath)
    val ec = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(2))
    new SimpleArtifactRepository(fsArtifactRepo, defaultArtifactRepo)(ec)
  }
}