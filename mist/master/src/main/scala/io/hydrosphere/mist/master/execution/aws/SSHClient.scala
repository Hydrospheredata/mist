package io.hydrosphere.mist.master.execution.aws

import com.decodified.scalassh._
import cats._
import cats.implicits._

import scala.util.Try

sealed trait SSHCmd
object SSHCmd {
  case class CopyFile(from: String, to: String) extends SSHCmd
  case class Exec(cmd: Seq[String]) extends SSHCmd
}

class SSHClient(host: String, user: String, keyPath: String) {

  val cfgProvider = HostConfig(
    login = PublicKeyLogin(user, keyPath),
    hostName = host,
    port = 22,
    hostKeyVerifier = HostKeyVerifiers.DontVerify
  )

  def install(cmds: Seq[SSHCmd]): Try[Unit] = {
    SSH(host, cfgProvider) { client =>
     cmds.map {
       case SSHCmd.CopyFile(from, to) => client.upload(from, to)
       case SSHCmd.Exec(cmd) => client.exec(Command(cmd.mkString(" "))).map(_ => ())
     }.toList.sequence
    }.map(_ => ())
  }
}

