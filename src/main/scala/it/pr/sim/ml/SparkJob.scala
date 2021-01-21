package it.pr.sim.ml

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.util.Try
import org.apache.hadoop.fs.{FileSystem, Path}
import java.util.Properties
import org.apache.spark.SparkFiles
import org.apache.log4j.{Level, Logger}
import it.pr.sim.ml.helper.DfReaderHelper
import it.pr.sim.ml.exception.Exception._


abstract class SparkJob(appName:String) extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  val log = org.slf4j.LoggerFactory.getLogger("it.pr.sim.ml")
  
  val applicationName = appName
  
  if(args.size < 2) {
    log.error("In order to execute this spark job please provide the following parameters: ")
    log.error("repoPath: path of HDFS root folder or local root folder")
    log.error("propPath: path of configurations file e.g. '/Resources/confs/properties.conf'")
    sys.exit(1)
  }
  
    val repoPath  = args(0)
    val propPath= args(1)
    val thriftUrl  = Try(args(2)).getOrElse(null)
    val defaultFS = Try(args(3)).getOrElse(null)
    val rmAddress = Try(args(4)).getOrElse(null)
  
  log.info(s"repoPath: $repoPath")
  log.info(s"thriftUrl: $thriftUrl")
  log.info(s"propPath: $propPath")
  log.info(s"defaultFS: $defaultFS")
  log.info(s"resmanage: $rmAddress")
  
  
  val conf: SparkConf = new SparkConf()
    conf.setAppName(applicationName)

    
    if(thriftUrl != null) conf.set("hive.metastore.uris", thriftUrl)
    if(defaultFS != null) conf.set("spark.hadoop.fs.defaultFS", defaultFS)
    if(rmAddress != null) conf.set("spark.hadoop.yarn.resourcemanager.address", rmAddress)
                                 
  implicit val spark: SparkSession = SparkSession.builder()
                                 .config(conf)
                                 .enableHiveSupport()
                                 .getOrCreate()
  log.info("SparkSession.builder")
  
  implicit val sc = spark.sparkContext
  
    /**
     * Questo metodo e' implementato da tutte le classi che estendono SparkJob
     */
    def run(): Unit 
    
    val sourceHeper = new DfReaderHelper()
    implicit val env: Properties = new Properties()
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
       * query 
       */
      val queryProp = env.getProperty("sbi.scripts.query")
      val hdfs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
//      env.load(hdfs.open(new Path(repoPath + queryProp)).getWrappedStream)
//      hdfs.close()
      try{
        env.load(hdfs.open(new Path(repoPath + queryProp)).getWrappedStream)
      }catch{
        case e: Exception =>{
          val fis = new java.io.FileInputStream(new java.io.File(repoPath + queryProp))
          env.load(fis)
          fis.close()
        }
      }finally{
        hdfs.close()
      }
      
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