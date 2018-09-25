package io.hydrosphere.mist.master.execution.aws

import software.amazon.awssdk.services.emr.{model => emodel }
sealed trait EMRStatus

object EMRStatus {

  case object Starting extends EMRStatus
  case object Started extends EMRStatus
  case object Terminated extends EMRStatus
  case object Terminating extends EMRStatus

  def fromCluster(cluster: emodel.Cluster): EMRStatus =  cluster.status().state() match {
    case emodel.ClusterState.STARTING | emodel.ClusterState.BOOTSTRAPPING => Starting
    case emodel.ClusterState.WAITING | emodel.ClusterState.RUNNING => Started
    case emodel.ClusterState.TERMINATED | emodel.ClusterState.TERMINATED_WITH_ERRORS => Terminated
    case emodel.ClusterState.TERMINATING => Terminating
    case emodel.ClusterState.UNKNOWN_TO_SDK_VERSION => throw new RuntimeException("Used amazon sdk version is incompatible with aws")
  }

}

