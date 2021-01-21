package it.pr.sim.ml.dataPreparation

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

import it.pr.sim.ml.exception.Exception._

class ExecuteQuery(applicationName:String, query:String) extends Serializable {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val log = org.slf4j.LoggerFactory.getLogger("it.pr.sim.ml.dataPreparation")
    
  def run(isTruncate:Boolean)(implicit spark:SparkSession) : Unit = {
    var esito = ""
    
    log.info(s"------------------------------------------------------------------")
			log.info(s"----                                                          ----")
			log.info(s"----   $applicationName start...")
			log.info(s"----                                                          ----")
			log.info(s"------------------------------------------------------------------")
    
			try{
			  log.info(s"query: \n$query")

			  
			  val df = spark.sql(query).coalesce(1)
			  log.info("spark.sql OK")
			  
			  
        esito = "OK"
      } catch {
  			case e: Exception => {
  				throw new FormatWriteException(e.getMessage, e)
  			}
  			esito = "KO"
			}finally{
			  log.info(s"------------------------------------------------------------------")
  			log.info(s"----                                                          ----")
  			log.info(s"----      $applicationName $esito")
  			log.info(s"----                                                          ----")
  			log.info(s"------------------------------------------------------------------")
			}
  }
}