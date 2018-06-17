package test

import edu.bupt.iot.util.json.JSONParse

import scala.util.parsing.json.JSON

object Test {
  /*
  def regixJsonMap(json : Option[Any]) = json match {
    case Some(map: Map[String, Any]) => map
    case other =>
      println("Unknown data structure: " + other)
      Map[String, Any]()
  }
  def regixJsonList(json : Option[Any]) =  json match {
    case Some(list: List[Map[String, Any]]) => list
    case other =>
      println("Unknown data structure: " + other)
      List[Map[String, Any]]()
  }
  */

  def main(args: Array[String]): Unit = {
    /*
    println("Hello Scala!")
    println(new StringFormat((123).toString).formatted("%13s").replaceAll(" ", "0"))
    val start = new StringFormat((if (1 > 3600000) 1 - 3600000 else 0).toString).formatted("%13s").replaceAll(" ", "0")
    println(start)
    val dataFilePre = "123"
    println(s"hdfs://master:9000/data/$dataFilePre*")
    */
    println("-3.32855274E8".toDouble)
    var s = "{\"deviceId\":\"21b94370-6252-11e8-b8df-59c2cc02320f\",\"tenantId\":2,\"deviceType\":\"default\",\"data\":[{\"ts\":1527503755890,\"key\":\"humidity\",\"value\":20}]}"
    //s = "{\"deviceId\":\"5\",\"tenantId\":2,\"deviceType\":"default","data":[{"key":"x","value":"89","ts":"1527067186"}]}
    //println(s)
    /*
    val js = JSON.parseFull(s)
    //val jsr = JSON.parseRaw("{\n\t\"deviceId\": \"1\",\n\t\"tenantId\": \"1\",\n\t\"data\": [{\n\t\t\"key\": \"x\",\n\t\t\"ts\": \"1524708830000\",\n\t\t\"value\": \"2.00\"\n\t}]\n}")
    //println(js.getClass)
    //println(JSON.parseFull(jsr.get.toString()).get)
    val map = regixJsonMap(js)
    println(map.get("data"))
    val list = regixJsonList(map.get("data"))
    println(list)
    println(list.head.get("ts").get.toString)
    */
    try {
      val json = JSONParse.str2Json(s)
      val map = JSONParse.json2Map(json)
      val list = JSONParse.json2List(map.get("data"))

      println(map("tenantId").asInstanceOf[Number].longValue.toString)
      println(map("deviceType").toString)
      println(map("deviceId").toString)
      println(list.head("ts").asInstanceOf[Number].longValue.toString)
      println(list.head("key").toString)
      println(list.head("value").asInstanceOf[Number].doubleValue.toString)
      println(map("data"))
      val value = list.head("value").getClass
      print(s + '\n' + json + '\n' + map + '\n' + list + '\n' + value + '\n')
    } catch {
      case e : Throwable  => {
        println(e)
        e.printStackTrace()
      }
    } finally {
    }
    /*
    val test = Array("","123;456;","1;2;;;;",";0;9;9;","9;8",";77;66")
    val strArr = test.filter(!_.equals(""))
    val dataArr = strArr.flatMap(_.split(";"))
    dataArr.foreach(println(_))
    //test.split(";").foreach(println(_))
    println("end")
    */
  }
}
