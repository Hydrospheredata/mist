package com.provectus.mist.jobs

import com.provectus.mist.Specification

private[mist] trait JobSpecification extends Specification[Job]

/** Predicate for search [[Job]] in repository by its id
  *
  * @param id job id
  */
private[mist] class JobByIdSpecification(id: String) extends JobSpecification{
  override def specified(job: Job): Boolean = {
    job.id == id
  }
}
