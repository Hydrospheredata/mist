package com.provectus.lymph.actors.tools

/** JSON schemas to validate incoming messages */
private[actors] object JSONSchemasPy {

  /** Job Requesting options */
  val jobRequest =
    """{
      | "title": "Async Job Request",
      | "type": "object",
      | "properties": {
      |   "pyPath": {"type": "string"},
      |   "python": {"type": "boolean"},
      |   "parameters": {"type": "object"},
      |   "external_id": {"type": "string"}
      | },
      | "required": ["pyPath", "python"]
      |}
    """.stripMargin
}