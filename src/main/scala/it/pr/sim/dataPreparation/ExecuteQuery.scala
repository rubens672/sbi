package it.pr.sim.dataPreparation

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

import it.pr.sim.exception.Exception._

class ExecuteQuery(applicationName:String, truncateQuery:String, query:String) extends Serializable {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val log = org.slf4j.LoggerFactory.getLogger("it.pr.sim.dataPreparation")
    
  def run(isTruncate:Boolean)(implicit spark:SparkSession) : Unit = {
    var esito = ""
    
    log.info(s"------------------------------------------------------------------")
			log.info(s"----                                                          ----")
			log.info(s"----   $applicationName start...")
			log.info(s"----                                                          ----")
			log.info(s"------------------------------------------------------------------")
    
			try{
			  log.info(s"truncateQuery: $truncateQuery")
			  log.info(s"query: \n$query")

			  
			  if(isTruncate){
			    val dfTrucate = spark.sql(truncateQuery)
			    log.info("dfTrucate OK")
			  }
			  
			  
			  val df = spark.sql(query)
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