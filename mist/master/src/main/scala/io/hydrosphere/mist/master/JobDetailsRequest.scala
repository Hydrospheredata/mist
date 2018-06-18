package io.hydrosphere.mist.master

sealed trait FilterClause

object FilterClause {
  case class ByFunctionId(id: String) extends FilterClause
  case class ByWorkerId(id: String) extends FilterClause
  case class ByStatuses(statuses: Seq[JobDetails.Status]) extends FilterClause
}

case class JobDetailsRequest(
  limit: Int,
  offset: Int,
  filters: Seq[FilterClause]
) {

  def withFilter(f: FilterClause): JobDetailsRequest = copy(filters = f +: filters)
}

object JobDetailsRequest {
  def apply(limit: Int, offset: Int): JobDetailsRequest = JobDetailsRequest(limit, offset, Seq.empty)
}

case class JobDetailsResponse(
  jobs: Seq[JobDetails],
  total: Int
)

