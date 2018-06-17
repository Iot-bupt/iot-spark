package edu.bupt.iot.util.hdfs

import java.io.{ByteArrayInputStream, IOException}
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.io.IOUtils

object HdfsUtils {

  def getFS(): FileSystem = {
    System.setProperty("HADOOP_USER_NAME", HdfsConfig.getUserName())
    val conf = HdfsConfig.getConf()
    FileSystem.get(conf)
  }

  def closeFS(fileSystem: FileSystem) {
    if (fileSystem != null) {
      try {
        fileSystem.close()
      } catch {
        case e: IOException => e.printStackTrace()
      }
    }
  }

  def mkdir(hdfsFilePath: String) = {
    val fileSystem = getFS()
    try {
      val success = fileSystem.mkdirs(new Path(hdfsFilePath))
      if (success) {
        println("Create directory or file successfully")
      }
    } catch {
      case e: IllegalArgumentException => e.printStackTrace()
      case e: IOException => e.printStackTrace()
    } finally {
      this.closeFS(fileSystem)
    }
  }

  def rm(hdfsFilePath: String, recursive: Boolean): Unit = {
    val fileSystem = this.getFS()
    val path = new Path(hdfsFilePath)
    try {
      if (fileSystem.exists(path)) {
        val success = fileSystem.delete(path, recursive)
        if (success) {
          System.out.println("delete successfully")
        }
      }

    } catch {
      case e: IllegalArgumentException => e.printStackTrace()
      case e: IOException => e.printStackTrace()
    } finally {
      this.closeFS(fileSystem)
    }
  }

  def append(hdfsFilePath: String, data: String): Unit = {
    val fileSystem = this.getFS()
    val path = new Path(hdfsFilePath)
    val in = new ByteArrayInputStream((data).getBytes())
    //val out = fileSystem.append(path)
    var out:FSDataOutputStream = null
    if (!fileSystem.exists(path)) {
      out = fileSystem.create(path)
    } else {
      out = fileSystem.append(path)
    }
    try {
      IOUtils.copyBytes(in, out, 4096, true)
    } catch {
      case e: IOException =>
        e.printStackTrace()
    } finally {
      IOUtils.closeStream(in)
      IOUtils.closeStream(out)
      this.closeFS(fileSystem)
    }
  }

}
