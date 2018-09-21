package edu.bupt.iot.util.mysql

import java.sql.{Connection, DriverManager, ResultSet}

object MysqlConfig {
  //val dbc = "jdbc:mysql://39.104.165.155:3306/BUPT_IOT?user=root&password=root"
  val dbc = "jdbc:mysql://172.24.32.169:3306/BUPT_IOT?user=root&password=root"
  classOf[com.mysql.jdbc.Driver]

  def getConnection(): Connection = {
    val conn = DriverManager.getConnection(dbc)
    val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
    conn
  }
}
