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
import scala.util.Try
import it.pr.sim.schema.Schemas._
import it.pr.sim.SparkJob
import it.pr.sim.exception.Exception._

/**
## D_VALORI_ATM_POS
CREATE TABLE sbi.D_VALORI_ATM_POS(  
    cubeid string,  
    durata_originaria_della_applicazione string,  
    residenza string,  
    divisa string,  
    fenomeno_economico string,  
    localizzazione_dello_sportello string,  
    data_dell_osservazione timestamp,  
    ente_segnalante string,  
    valore bigint  
  )  
ROW FORMAT DELIMITED  
FIELDS TERMINATED BY '\t'  
STORED AS PARQUET  
LOCATION '/user/pr/sbi/D_VALORI_ATM_POS';
 */

object dValoriAtmPos extends SparkJob("Fonti Esterne SBI - D_VALORI_ATM_POS", "it.pr.sim.fontiEsterne.dValoriAtmPos") {
  
  	def run() : Unit = {
			log.info(s"------------------------------------------------------------------")
			log.info(s"----                                                          ----")
			log.info(s"----      $applicationName start...     ----")
			log.info(s"----                                                          ----")
			log.info(s"------------------------------------------------------------------")

		  try{
    		val dataelaborazione = new java.sql.Timestamp(System.currentTimeMillis())
    		val storedAs = env.getProperty("sbi.fonti.esterne.hadoop.format")
    		val mode = env.getProperty("sbi.fonti.esterne.hadoop.mode")
    		val csv_valori_atm_pos = repoPath.concat(env.getProperty("sbi.fonti.esterne.csv.valori_atm_pos"))
    		val d_valori_atm_pos = env.getProperty("sbi.fonti.esterne.hadoop.d_valori_atm_pos")
    
    		
    		log.info("sbi.fonti.esterne.csv.valori_atm_pos: " + csv_valori_atm_pos)
        log.info("sbi.fonti.esterne.hadoop.d_valori_atm_pos: " + d_valori_atm_pos)
        log.info("sbi.fonti.esterne.hadoop.format: " + storedAs)
        log.info("sbi.fonti.esterne.hadoop.mode: " + mode)
        
        
        val format = new SimpleDateFormat("dd/MM/yyyy")
        val toDate:String => Timestamp = ( f => new Timestamp(format.parse(f.toString()).getTime()) )
        
        
        log.info("select sparkDF OK")
        
        import spark.implicits._
  			val src = sourceHeper.readFromHDFS(csv_valori_atm_pos)	
  			val row = src.getLines().drop(1).map(f => org.apache.spark.sql.Row.fromSeq(f.split(";"))).toList
  			val rdd = spark.sparkContext.makeRDD(row)
          
        val val_atm_pos_rdd = rdd.map{row =>
            Try(Row(row.getAs[String](0),
                row.getAs[String](1),
                row.getAs[String](2),
                row.getAs[String](3),
                row.getAs[String](4),
                row.getAs[String](5), 
                toDate(row.getAs[String](6)), 
                row.getAs[String](7),
                row.getAs[String](8).toLong )).getOrElse(null)
        }.setName("lettura del dataframe").filter(row => row != null).setName("filtro dei record non validi")  
        
        log.info("create RDD OK")
        
  			
        val val_atm_pos_yarn_df = spark.createDataFrame(val_atm_pos_rdd, schemaValoriAtmPos)
        log.info("createDataFrame OK")
        
        val_atm_pos_yarn_df.write.format(storedAs).mode(mode).save(d_valori_atm_pos)
        log.info("save d_valori_atm_pos OK")
        
        esito = "OK"
      } catch {
  			case e: Exception => {
  				throw new FormatWriteException(e.getMessage, e)
  			}
  			esito = "KO"
			}
			log.info(s"------------------------------------------------------------------")
  		log.info(s"----                                                          ----")
  		log.info(s"----        $applicationName $esito         ----")
  		log.info(s"----                                                          ----")
  		log.info(s"------------------------------------------------------------------")
 	}
  
}