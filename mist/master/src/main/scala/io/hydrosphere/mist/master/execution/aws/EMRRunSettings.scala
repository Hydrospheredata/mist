package io.hydrosphere.mist.master.execution.aws

case class EMRRunSettings(
  name: String,
  keyPair: String,
  releaseLabel: String,
  subnetId: String,
  additionalGroup: String,
  emrRole: String,
  emrEc2Role: String,
  autoScalingRole: String
)

