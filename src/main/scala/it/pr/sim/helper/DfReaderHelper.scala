package it.pr.sim.helper

import org.apache.spark.sql.{ SparkSession}
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

class DfReaderHelper {
  
    def readFromHDFS(path:String)(implicit sparkSession:SparkSession): scala.io.BufferedSource = {
      val sc = sparkSession.sparkContext
      val hdfs: FileSystem = FileSystem.get(sc.hadoopConfiguration)
      val t = hdfs.open(new Path(path))
      val bs = new scala.io.BufferedSource(t)
      bs
  }
}