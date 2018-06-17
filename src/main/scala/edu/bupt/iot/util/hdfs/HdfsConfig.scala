package edu.bupt.iot.util.hdfs

import org.apache.hadoop.conf.Configuration

object HdfsConfig {

  final val HADOOP_USER_NAME = "spark"
  final val FS_DEFAULTFS = "hdfs://master:9000/"
  final val MAPRED_REMOTE_OS = "Linux"

  def getConf(): Configuration = {
    val conf = new Configuration()
    conf.set("fs.defaultFS", HdfsConfig.FS_DEFAULTFS)
    conf.set("mapred.remote.os", HdfsConfig.MAPRED_REMOTE_OS)
    conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true")
    conf.set("dfs.client.block.write.replace-datanode-on-failure.policy","NEVER")
    conf.setBoolean("dfs.support.append", true)
    conf
  }

  def getUserName(): String = {
    HADOOP_USER_NAME
  }

}
