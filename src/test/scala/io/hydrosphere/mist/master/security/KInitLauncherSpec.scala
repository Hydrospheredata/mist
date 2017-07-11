package io.hydrosphere.mist.master.security

import org.scalatest._

import scala.concurrent.Await
import scala.concurrent.duration._

class KInitLauncherSpec extends FunSpec with Matchers {

  it("should run loop") {
    val ps = KInitLauncher.LoopedProcess(Seq("pwd"), 1.second)
    ps.run()
    Await.result(ps.stop(), Duration.Inf)
  }

  it("should construct correct kinit cmd") {
    val ps = KInitLauncher.create("keytab", "principal", 1.second)
    ps.cmd should contain inOrderOnly("kinit", "-kt", "keytab", "principal")
  }
}
