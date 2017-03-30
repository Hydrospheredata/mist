package io.hydrosphere.mist.jobs

import io.hydrosphere.mist.jobs.resolvers.JobResolver
import io.hydrosphere.mist.jobs.runners.jar.{JobsLoader, JobClass}

import scala.util.{Failure, Success, Try}

sealed trait JobInfo {
  val definition: JobDefinition
}

/**
  * For python we can only provide info about configuration
  */
case class PyJobInfo(definition: JobDefinition) extends JobInfo

case class JvmJobInfo(
  definition: JobDefinition,
  jobClass: JobClass
) extends JobInfo


object JobInfo {

  def load(definition: JobDefinition): Try[JobInfo] = {
    val resolver = JobResolver.fromPath(definition.path)
    load(definition, resolver)
  }

  def load(definition: JobDefinition, resolver: JobResolver): Try[JobInfo] = {
    val resolveFile = Try { resolver.resolve() }
    resolveFile.flatMap(file => {
      file.getName match {
        case p if p.endsWith(".py") =>
          val info = new PyJobInfo(definition)
          Success(info)
        case p if p.endsWith(".jar") =>
          val inst = JobsLoader.fromJar(file).loadJobClass(definition.className)
          inst.map(i => new JvmJobInfo(definition, i))
        case p =>
          val msg = s"Unknown file format $p for $definition"
          Failure(new IllegalArgumentException(msg))
      }
    })
  }

}


