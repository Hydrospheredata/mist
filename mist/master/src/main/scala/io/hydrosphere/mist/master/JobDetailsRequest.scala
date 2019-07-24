package io.hydrosphere.mist.master

import cats.data.NonEmptyList
import java.io.File

sealed trait FilterClause

object FilterClause {
  case class ByFunctionId(id: String) extends FilterClause
  case class ByWorkerId(id: String) extends FilterClause
  case class ByStatuses(statuses: NonEmptyList[JobDetails.Status]) extends FilterClause
}

case class JobDetailsRequest(
  limit: Int,
  offset: Int,
  filters: Seq[FilterClause]
) { self =>

  def withFilter(f: FilterClause): JobDetailsRequest = copy(filters = f +: filters)
  def withOptFilter(optF: Option[FilterClause]): JobDetailsRequest = 
    optF.fold(self)(f => self.withFilter(f))
}

object JobDetailsRequest {
  def apply(limit: Int, offset: Int): JobDetailsRequest = JobDetailsRequest(limit, offset, Seq.empty)
}

case class JobDetailsResponse(
  jobs: Seq[JobDetails],
  total: Int
)

