package it.pr.sim

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.util.Try
import org.apache.hadoop.fs.{FileSystem, Path}
import java.util.Properties
import org.apache.spark.SparkFiles
import org.apache.log4j.{Level, Logger}
import it.pr.sim.exception.Exception._
import it.pr.sim.helper.DfReaderHelper
/*
 * repoPath = hdfs://cloudera001.binarioetico.it:8020/user/pr/spark
 * thriftUrl = thrift://cloudera001.binarioetico.it:9083
 * propPAth = /Resources/confs/properties.conf
 * defaultFS = cloudera001.binarioetico.it
 * rmAddress = cloudera001.binarioetico.it
 * 
 * hdfs://cloudera001.binarioetico.it:8020/user/pr/spark thrift://cloudera001.binarioetico.it:9083 /Resources/confs/properties.conf cloudera001.binarioetico.it cloudera001.binarioetico.it
 * 
 * hdfs://cloudera001.binarioetico.it:8020/user/pr/spark/Resources/confs/properties.conf
 */
abstract class SparkJob(appName:String, loggerName:String) extends App{
  
    Logger.getLogger("org").setLevel(Level.ERROR)
    val log = org.slf4j.LoggerFactory.getLogger(loggerName)
  
    val applicationName = appName
  
    if(args.size < 1) {
      log.error("In order to execute this spark job please provide the following parameters: ")
      log.error("repoPath: path of HDFS root folder or local root folder")
//      log.error("thriftUrl: thrift url e.g. thrift://master01:9083")
      sys.exit(1)
    }
    
    val repoPath  = args(0)
//    val thriftUrl = args(1)
    val thriftUrl = Try(args(1)).getOrElse(null)
    val propPath  = Try(args(2)).getOrElse(null)
    val defaultFS = Try(args(3)).getOrElse(null)
    val rmAddress = Try(args(4)).getOrElse(null)
    
    log.info(s"repoPath:  $repoPath")
    log.info(s"thriftUrl: $thriftUrl")
    log.info(s"propPath:  $propPath")
    log.info(s"defaultFS: $defaultFS")
    log.info(s"rmAddress: $rmAddress")
    
    val conf: SparkConf = new SparkConf()
    conf.setAppName(applicationName)
    
                                
    if(thriftUrl != null) conf.set("hive.metastore.uris", thriftUrl) 
    if(defaultFS != null) conf.set("spark.hadoop.fs.defaultFS", defaultFS)
    if(rmAddress != null) conf.set("spark.hadoop.yarn.resourcemanager.address", rmAddress)
  
  
    implicit val spark: SparkSession = SparkSession.builder()
                                 .appName(applicationName)
                                 .config(conf)
                                 .enableHiveSupport()
                                 .getOrCreate()
                                 
     
    def sparkUser(): String = {
      val sparkUser = Option {
  				Option(System.getProperty("user.name")).getOrElse(System.getenv("SPARK_USER"))
  			}.getOrElse {
  				Option("SPARK_UNKNOWN_USER")
  			}
  		sparkUser.toString()
    }
    
    log.info("spark.master: " + spark.conf.get("spark.master"))
    log.info("spark.submit.deployMode: " + spark.conf.get("spark.submit.deployMode"))
    log.info("appName: " + applicationName)
    log.info("hadoop.user.name: " + sparkUser)
    log.info("SparkSession builder OK")
    
    /**
     * Questo metodo e' implementato da tutte le classi che estendono SparkJob
     */
    def run(): Unit 
    
    val sourceHeper = new DfReaderHelper()
    val env: Properties = new Properties()
    var esito:String = ""
    
    try{
     
      val filename = SparkFiles.get("properties.conf")
      log.info(s"filename: $filename")
      val file = new java.io.File(filename)
      
      if(file.exists()){
        val fis = new java.io.FileInputStream(filename)
        env.load(fis)
        fis.close()
      }else{
        val hdfs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
        env.load(hdfs.open(new Path(repoPath + propPath)).getWrappedStream)
        hdfs.close()
      }
      
      log.info(s"filename exist: " + file.exists())
      
      /**
       * query per la geolocalizzazione
       */
      val queryProp = env.getProperty("sbi.scripts.query")
      val hdfs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      env.load(hdfs.open(new Path(repoPath + queryProp)).getWrappedStream)
      hdfs.close()
      
      
      /**
       * query per le truncate di data preparation
       */
      val queryTrucate = env.getProperty("sbi.data.preparation.truncate")
      val hdfsTrucate: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      env.load(hdfsTrucate.open(new Path(repoPath + queryTrucate)).getWrappedStream)
      hdfsTrucate.close()
      
      
      /** ******** **/
      run()
      /** ******** **/
      
      
    } catch {
        case e: ApplicationException => {
          val msg = e.getMessage
          val errorCode = e.errorCode
          log.error(s"errorCode: $errorCode - $msg")
          throw e
        }
  			case e: Exception => {
  			  val msg = e.getMessage
  			  log.error(s"generic error!! - $msg")
  			  throw e
  			}
    }finally{
      spark.close()
      log.info(s"SparkSession close")
    }
     
}