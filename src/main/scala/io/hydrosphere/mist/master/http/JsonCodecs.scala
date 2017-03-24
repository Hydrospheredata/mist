package io.hydrosphere.mist.master.http

import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport
import io.hydrosphere.mist.jobs.JobDetails

trait JsonCodecs extends FailFastCirceSupport {

  import io.circe._

  implicit val encodeJobStatus: Encoder[JobDetails.Status] =
    Encoder.instance(s => Json.fromString(s.toString))

  implicit val decodeJobStatus: Decoder[JobDetails.Status] =
    Decoder.decodeString.map(s => JobDetails.Status(s))

}

object JsonCodecs extends JsonCodecs

