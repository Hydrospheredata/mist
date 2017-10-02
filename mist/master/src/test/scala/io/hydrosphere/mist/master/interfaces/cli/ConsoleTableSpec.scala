package io.hydrosphere.mist.master.interfaces.cli

import org.scalatest.FunSpec

class ConsoleTableSpec extends FunSpec {

  it("should pretty print") {
    val table = ConsoleTable(
      headers = List("ID", "NAME"),
      rows = List(Row.create("1", "first"), Row.create("2", "second"))
    )

    val expect =
      """+----+--------+
        || ID | NAME   |
        |+----+--------+
        || 1  | first  |
        || 2  | second |
        |+----+--------+
        |""".stripMargin

    assert(expect == table.prettyPrint)
  }
}
