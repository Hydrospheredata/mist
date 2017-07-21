package io.hydrosphere.mist.jobs

import io.hydrosphere.mist.jobs.resolvers.JobResolver
import io.hydrosphere.mist.jobs.jar.{JobsLoader, JobClass}

import scala.util.{Failure, Success, Try}

sealed trait JobInfo

case object PyJobInfo extends JobInfo
case class JvmJobInfo(jobClass: JobClass) extends JobInfo

object JobInfo {

  def load(name: String, path: String, className: String): Try[JobInfo] = {
    val resolver = JobResolver.fromPath(path)
    load(name, path, className, resolver)
  }

  def load(name: String, path: String, className: String, resolver: JobResolver): Try[JobInfo] = {
    val resolveFile = Try { resolver.resolve() }
    resolveFile.flatMap(file => {
      file.getName match {
        case p if p.endsWith(".py") =>
          val info = PyJobInfo
          Success(info)
        case p if p.endsWith(".jar") =>
          val inst = JobsLoader.fromJar(file).loadJobClass(className)
          inst.map(i => JvmJobInfo(i))
        case p =>
          val msg = s"Unknown file format $p for $path"
          Failure(new IllegalArgumentException(msg))
      }
    })
  }

}


