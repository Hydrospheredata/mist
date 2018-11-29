package io.hydrosphere.mist.master.execution.aws

import java.nio.file.Paths

case class TransferParams(
  agentJar: String,
  workerJar: String,
  targetDir: String
)

case class AgentRunParams(
  agentId: String,
  masterAddress: String,
  accessKey: String,
  secretKey: String,
  region: String,
  clusterId: String
)

object AgentInstall {

  def sshCommands(
    transferParams: TransferParams,
    agentRunParams: AgentRunParams
  ): Seq[SSHCmd] = {

    import transferParams._
    import agentRunParams._

    val agentRemoteJar = s"$targetDir/mist-agent.jar"
    Seq(
      SSHCmd.Exec(Seq("mkdir", targetDir)),
      SSHCmd.CopyFile(agentJar, s"$targetDir/mist-agent.jar"),
      SSHCmd.CopyFile(workerJar, s"$targetDir/mist-worker.jar"),
      SSHCmd.Exec(Seq(
        "java", "-cp", agentRemoteJar, "io.hydrosphere.mist.agent.ClusterAgent",
        masterAddress, agentId, accessKey, secretKey, region, clusterId,
        s"1>$targetDir/out.log", s"2>$targetDir/out.log", "&"
      ))
    )
  }

}
