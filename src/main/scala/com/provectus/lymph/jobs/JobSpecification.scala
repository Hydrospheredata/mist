package com.provectus.lymph.jobs

import com.provectus.lymph.Specification

private[lymph] trait JobSpecification extends Specification[Job]

private[lymph] class JobByIdSpecification(id: String) extends JobSpecification{
  override def specified(job: Job): Boolean = {
    job.id == id
  }
}
