package edu.bupt.iot.util.json

import scala.util.parsing.json.JSON

object JSONParse {
  def str2Json(str : String) : Option[Any] =  {
    JSON.parseFull(str)
  }
  def json2Map(json : Option[Any]) : Map[String, Any] = json match {
    case Some(map: Map[String, Any]) => map
    case other =>
      println("Unknown data structure: " + other)
      Map[String, Any]()
  }
  def json2List(json : Option[Any]) : List[Map[String, Any]] =  json match {
    case Some(list: List[Map[String, Any]]) => list
    case other =>
      println("Unknown data structure: " + other)
      List[Map[String, Any]]()
  }
}
