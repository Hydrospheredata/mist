package io.hydrosphere.mist.master.execution.aws

case class EMRRunSettings(
  name: String,
  keyPair: String,
  releaseLabel: String,
  masterInstanceType: String,
  slaveInstanceType: String,
  instanceCount: Int,
  subnetId: String,
  additionalGroup: String,
  emrRole: String,
  emrEc2Role: String
)

