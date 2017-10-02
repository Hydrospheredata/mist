package io.hydrosphere.mist.master.interfaces.cli

import org.scalatest.{Matchers, FunSpec}
import org.scalatest.prop.TableDrivenPropertyChecks._

class CommandSpec extends FunSpec with Matchers {


  val expected = Table(
    ("input", "cmd"),
    (Command.listJobs, RunningJobsCmd),
    (s"${Command.stopWorker} name", StopWorkerCmd("name")),
    (Command.listWorkers, ListWorkersCmd),
    (Command.listRouters, ListRoutesCmd),
    (Command.stopAllWorkers, StopAllWorkersCmd),
    (s"${Command.stopJob} namespace job", StopJobCmd("namespace", "job")),
    (Command.exit, Exit),
    (Command.startJob + " xxx '{\"a\":\"b\"}'", StartJobCmd("xxx", None, Map("a" -> "b")))
  )

  it("should parse input") {
    forAll(expected) { (input: String, cmd: Command) =>
      Command.parse(input) shouldBe Right(cmd)
    }
  }
}
