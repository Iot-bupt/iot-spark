package edu.bupt.iot.util.hdfs

object Test {
  def main(args: Array[String]): Unit = {
    //HdfsUtils.append("/test/test.txt", "I love liwan") 1522512000
    val types = Array("temperature", "humidity", "pressure", "deformation", "light", "velocity")

    for (i <- 0 to 56){
      val baseDay = 1522512000 + 3600 * 24 * i
      for (j <- 0 to 23){
        val baseHour = baseDay + j * 3600
        for (userId <- 0 to 10){
          val data = new StringBuilder
          for (cnt <- 0 to 10){
            val deviceType = types((new util.Random).nextInt(6))
            val deviceId = userId * 10 + (new util.Random).nextInt(10)
            val value = (new util.Random).nextGaussian() * 16 + 50
            val time = ((new util.Random).nextInt(3600) + baseHour).toLong * 1000.toLong
            data.append(userId + "," + deviceType + "," + deviceId + "," + value + "," + time + "\n")
          }
          HdfsUtils.append(s"/data/device-data-${baseHour.toLong * 1000.toLong}", data.toString)
        }
      }
    }
  }
}
