package com.provectus.lymph.actors.tools

/** JSON schemas to validate incoming messages */
private[actors] object JSONSchemas {

  /** Job Requesting options */
  val jobRequest =
    """{
      | "title": "Async Job Request",
      | "type": "object",
      | "properties": {
      |   "jarPath": {"type": "string"},
      |   "className": {"type": "string"},
      |   "parameters": {"type": "object"},
      |   "external_id": {"type": "string"}
      | },
      | "required": ["jarPath", "className"]
      |}
    """.stripMargin
}