package it.pr.sim.fontiEsterne

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.text.SimpleDateFormat
import java.sql.Timestamp
import org.apache.spark.sql.Row
import org.apache.log4j.{Level, Logger}

import it.pr.sim.schema.Schemas._
import it.pr.sim.SparkJob
import it.pr.sim.exception.Exception._

/**
CREATE TABLE sbi.D_POPOLAZIONE(  
  itter107 string,  
  territorio string,   
  tipo_dato15 string,   
  tipo_di_indicatore_demografico string,   
  sexist1 string,  
  sesso string,  
  eta1 string,  
  eta string,  
  statciv2 string,  
  stato_civile string,  
  time int,  
  seleziona_periodo string,  
  value bigint)  
ROW FORMAT DELIMITED  
FIELDS TERMINATED BY '\t'  
STORED AS PARQUET  
LOCATION '/user/pr/sbi/D_POPOLAZIONE';
 */

object dPopolazione extends SparkJob("Fonti Esterne SBI - D_POPOLAZIONE", "it.pr.sim.fontiEsterne.dPopolazione") {
	 
	
	def run() : Unit = {
    log.info(s"------------------------------------------------------------------")
  	log.info(s"----                                                          ----")
  	log.info(s"----        $applicationName start...      ----")
  	log.info(s"----                                                          ----")
  	log.info(s"------------------------------------------------------------------")
  	
    try{  	
    		val dataelaborazione = new java.sql.Timestamp(System.currentTimeMillis())
    		val storedAs = env.getProperty("sbi.fonti.esterne.hadoop.format")
    		val mode = env.getProperty("sbi.fonti.esterne.hadoop.mode")
    		val csv_popolazione = repoPath.concat(env.getProperty("sbi.fonti.esterne.csv.popolazione"))
    		val d_popolazione = env.getProperty("sbi.fonti.esterne.hadoop.d_popolazione")
    
    		log.info("sbi.fonti.esterne.csv.popolazione: " + csv_popolazione)
        log.info("sbi.fonti.esterne.hadoop.d_popolazione: " + d_popolazione)
        log.info("sbi.fonti.esterne.hadoop.format: " + storedAs)
        log.info("sbi.fonti.esterne.hadoop.mode: " + mode)
      	
      	
        val src = sourceHeper.readFromHDFS(csv_popolazione)	
        val row = src.getLines().drop(1).map(f => Row.fromSeq(f.split(","))).toList
         
        val sparkDFRDD = spark.sparkContext.makeRDD(row).map(row => {
          try{
                                                    Row(row.getAs[String](0).replaceAll("\"", ""),
                                                        row.getAs[String](1).replaceAll("\"", ""),
                                                        row.getAs[String](2).replaceAll("\"", ""),
                                                        row.getAs[String](3).replaceAll("\"", ""),
                                                        row.getAs[String](4).replaceAll("\"", ""),
                                                        row.getAs[String](5).replaceAll("\"", ""),
                                                        row.getAs[String](6).replaceAll("\"", ""),
                                                        row.getAs[String](7).replaceAll("\"", ""),
                                                        row.getAs[String](8).replaceAll("\"", ""),
                                                        row.getAs[String](9).replaceAll("\"", ""),
                                                        row.getAs[String](10).replaceAll("\"", "").toInt,
                                                        row.getAs[String](11).replaceAll("\"", ""),
                                                        row.getAs[String](12).replaceAll("\"", "").toLong)
           } catch {
            	case e: Exception => {
            		null
            	}
        	 }}).setName("lettura del dataframe").filter(row => row != null).setName("filtro dei record non validi")
        	 
        log.info("read csv OK")
        
        
        val pop_yarn_df = spark.createDataFrame(sparkDFRDD, schemaDPopolazione).where("sexist1=9 AND sesso='totale' AND statciv2=99")
    	  log.info("createDataFrame OK")
        
        
        log.info("sparkDF OK")
        
        
        pop_yarn_df.write.format(storedAs).mode(mode).save(d_popolazione)
        log.info("save d_popolazione OK")
        
        
        esito = "OK"
     } catch {
  			case e: Exception => {
  				throw new FormatWriteException(e.getMessage, e)
  			}
  			esito = "KO"
			}finally{
			  log.info(s"------------------------------------------------------------------")
      	log.info(s"----                                                          ----")
      	log.info(s"----          $applicationName $esito          ----")
      	log.info(s"----                                                          ----")
      	log.info(s"------------------------------------------------------------------")
			}

  }
}







