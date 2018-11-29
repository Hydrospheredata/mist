package io.hydrosphere.mist.master.execution.aws

import software.amazon.awssdk.services.emr.{model => emodel }

case class EmrInfo(
  id: String,
  masterPublicDnsName: String,
  status: EMRStatus
)

object EmrInfo {

  def fromCluster(cluster: emodel.Cluster): EmrInfo =
    EmrInfo(cluster.id(), cluster.masterPublicDnsName(), EMRStatus.fromCluster(cluster))

}
