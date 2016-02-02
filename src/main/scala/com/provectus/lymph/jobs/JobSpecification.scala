package com.provectus.lymph.jobs

import com.provectus.lymph.Specification

private[lymph] trait JobSpecification extends Specification[LymphJob]

private[lymph] class JobByIdSpecification(id: String) extends JobSpecification{
  override def specified(job: LymphJob): Boolean = {
    job.id == id
  }
}
