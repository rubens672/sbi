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
## D_VALORE_FINANZIARIO
CREATE TABLE sbi.D_VALORE_FINANZIARIO(  
    data_osservazione timestamp,  
    totale_circolante bigint  
  )  
ROW FORMAT DELIMITED  
FIELDS TERMINATED BY '\t'  
STORED AS PARQUET  
LOCATION '/user/pr/sbi/D_VALORE_FINANZIARIO';
 */

object dValoreFinanziario extends SparkJob("Fonti Esterne SBI - D_VALORE_FINANZIARIO", "it.pr.sim.fontiEsterne.dValoreFinanziario") {


	def run() : Unit = {
			log.info(s"------------------------------------------------------------------")
			log.info(s"----                                                          ----")
			log.info(s"----     $applicationName start...  ----")
			log.info(s"----                                                          ----")
			log.info(s"------------------------------------------------------------------")

    try{
					val dataelaborazione = new java.sql.Timestamp(System.currentTimeMillis())
					val storedAs = env.getProperty("sbi.fonti.esterne.hadoop.format")
					val mode = env.getProperty("sbi.fonti.esterne.hadoop.mode")
					val csv_valore_finanziario = repoPath.concat(env.getProperty("sbi.fonti.esterne.csv.valore_finanziario"))
					val d_valore_finanziario = env.getProperty("sbi.fonti.esterne.hadoop.d_valore_finanziario")

					log.info("sbi.fonti.esterne.valore.finanziario: " + csv_valore_finanziario)
          log.info("sbi.fonti.esterne.hadoop.d_valore_finanziario: " + d_valore_finanziario)
          log.info("sbi.fonti.esterne.hadoop.format: " + storedAs)
          log.info("sbi.fonti.esterne.hadoop.mode: " + mode)


					val format = new SimpleDateFormat("yyyy-MM-dd")

					def toDate(date:String): Timestamp = {
									try{
										new Timestamp(format.parse(date).getTime())
									} catch {
									case e: Exception => { null }
									}
					}

					val round:String => Long = ( 
							f => ((BigDecimal(f.replace(",","."))
									.setScale(0, BigDecimal.RoundingMode.HALF_UP).toLong) * 1000000) )
									
									
          import spark.implicits._
    			val src = sourceHeper.readFromHDFS(csv_valore_finanziario)	
    			val row = src.getLines().drop(1).map(f => Row.fromSeq(f.split(";"))).toList
    			val rdd = spark.sparkContext.makeRDD(row).map{row =>      
							Try(Row(toDate(row.getAs[String](0)),
									round(row.getAs[String](1))
									)).getOrElse(null)
					}.setName("lettura del dataframe").filter(row => row != null).setName("filtro dei record non validi")
          log.info("create RDD OK")

					
					val df = spark.createDataFrame(rdd, schemaValoreFinanziario)
					log.info("createDataFrame OK")

          df.write.format(storedAs).mode(mode).save(d_valore_finanziario)
          log.info("save d_valore_finanziario OK")
          
          
        esito = "OK"
      } catch {
  			case e: Exception => {
  				throw new FormatWriteException(e.getMessage, e)
  			}
  			esito = "KO"
			}finally{
			  	log.info(s"------------------------------------------------------------------")
					log.info(s"----                                                          ----")
					log.info(s"----      $applicationName $esito       ----")
					log.info(s"----                                                          ----")
					log.info(s"------------------------------------------------------------------")
			}

	}
}