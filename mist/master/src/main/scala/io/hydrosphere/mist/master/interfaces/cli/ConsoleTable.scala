package io.hydrosphere.mist.master.interfaces.cli

/**
  * Table data representation for pretty-printing cli command results
  */
case class ConsoleTable(
  headers: Seq[String],
  rows: Seq[Row]) {

  def prettyPrint: String = {
    def printBorder(sizes: Seq[Int]): String = {
      "+" + sizes.map(i => "-" * i + "+").mkString + "\n"
    }

    def printRow(sizes: Seq[Int], data: Seq[String]) = {
      "|" + data.zipWithIndex.map({case (s, i) =>
        " " + s + " " *  (sizes(i) - s.length - 1) + "|"
      }).mkString + "\n"
    }

    val columnSizes = (rows.map(_.data) :+ headers).transpose.map(_.map((_: String).length).max + 2)

    val builder = new StringBuilder()
    builder.append(printBorder(columnSizes))
    builder.append(printRow(columnSizes, headers))
    builder.append(printBorder(columnSizes))
    builder.append(rows.map({ row => printRow(columnSizes, row.data)}).mkString)
    builder.append(printBorder(columnSizes))

    builder.toString()
  }
}


case class Row(data: Seq[String])

object Row {

  def create(data: String*):Row = Row(data)

}
