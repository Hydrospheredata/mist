package com.provectus.lymph.actors.tools

import com.fasterxml.jackson.databind.{JsonNode, JsonMappingException}
import com.github.fge.jsonschema.main.JsonSchemaFactory

import org.json4s._
import org.json4s.jackson.JsonMethods._

private[actors] object JSONValidator {

  // https://gist.github.com/cjwebb/7e444eb36ec92fb904fd

  def validate(json: String, jsonSchema: String):Boolean = {

    if (json == null) {
      return false
    }

    val schema: JsonNode = asJsonNode(parse(jsonSchema))

    var instance: JsonNode = null
    try {
      instance = asJsonNode(parse(json))
    } catch {
      case _: JsonMappingException => return false
    }

    val validator = JsonSchemaFactory.byDefault().getValidator

    val processingReport = validator.validate(schema, instance)

    processingReport.isSuccess
  }

}
