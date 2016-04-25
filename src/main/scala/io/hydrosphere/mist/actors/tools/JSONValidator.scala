package io.hydrosphere.mist.actors.tools

import com.fasterxml.jackson.databind.{JsonNode, JsonMappingException, ObjectMapper}
import com.github.fge.jsonschema.main.JsonSchemaFactory

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Json4sScalaModule

/** JSON validator borrowed from https://gist.github.com/cjwebb/7e444eb36ec92fb904fd */
private[actors] object JSONValidator {

  private[this] lazy val _defaultMapper = {
    val m = new ObjectMapper()
    m.registerModule(new Json4sScalaModule)
    m
  }

  def validate(json: String, jsonSchema: String):Boolean = {

    if (json == null) {
      return false
    }

//    val schema: JsonNode = asJsonNode(parse(jsonSchema))
    val schema = mapper.valueToTree[JsonNode](parse(json))

    var instance: JsonNode = null
    try {
      //instance = asJsonNode(parse(json))
      instance = mapper.valueToTree[JsonNode](parse(json))
    } catch {
      case _: JsonMappingException => return false
    }

    val validator = JsonSchemaFactory.byDefault().getValidator

    val processingReport = validator.validate(schema, instance)

    processingReport.isSuccess
  }

}
