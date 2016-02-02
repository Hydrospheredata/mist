package com.provectus.lymph.actors.tools

private[actors] object JSONSchemas {

  val asyncJobRequest =
    """{
      | "title": "Async Job Request",
      | "type": "object",
      | "properties": {
      |   "jarPath": {"type": "string"},
      |   "className": {"type": "string"},
      |   "parameters": {"type": "object"}
      | },
      | "required": ["jarPath", "className"]
      |}
    """.stripMargin
}