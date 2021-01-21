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
## D_SUCCURSALI_BANCHE
CREATE TABLE sbi.D_SUCCURSALI_BANCHE(  
    id_int bigint,  
    den_160 string,  
    cod_mecc string,  
    id_sede string,  
    cab_sede string,  
    caus_aper string,  
    des_caus_aper string,  
    data_i_oper timestamp,  
    data_f_oper timestamp,  
    caus_chius string,  
    des_caus_chius string,  
    indirizzo string,  
    cap string,  
    frazione string,  
    cab_com_ita string,  
    des_com_ita string,  
    cod_loc_estera string,  
    des_loc_estera string,  
    stato bigint,  
    des_stato string,  
    tipo_sede string,  
    des_tipo_sede string,  
    data_i_val timestamp,  
    data_f_val timestamp  
  )  
ROW FORMAT DELIMITED  
FIELDS TERMINATED BY '\t'  
STORED AS PARQUET  
LOCATION '/user/pr/sbi/D_SUCCURSALI_BANCHE';
 */

object dSuccursaliBanche extends SparkJob("Fonti Esterne SBI - D_SUCCURSALI_BANCHE", "it.pr.sim.fontiEsterne.dSuccursaliBanche") {

	def run() : Unit = {
			log.info(s"------------------------------------------------------------------")
			log.info(s"----                                                          ----")
			log.info(s"----     $applicationName start...   ----")
			log.info(s"----                                                          ----")
			log.info(s"------------------------------------------------------------------")  	

    	try{	
    			val dataelaborazione = new java.sql.Timestamp(System.currentTimeMillis())
    			val storedAs = env.getProperty("sbi.fonti.esterne.hadoop.format")
    			val mode = env.getProperty("sbi.fonti.esterne.hadoop.mode")
    			val csv_succursali_banche = repoPath.concat(env.getProperty("sbi.fonti.esterne.csv.succursali_banche"))
    			val d_succursali_banche = env.getProperty("sbi.fonti.esterne.hadoop.d_succursali_banche")
    
    			log.info("sbi.fonti.esterne.csv.succursali_banche: " + csv_succursali_banche)
    			log.info("sbi.fonti.esterne.hadoop.d_succursali_banche: " + d_succursali_banche)
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
    
    			import scala.util.Try
    			val refill:String => String = ( f => Try("00000".concat(f)  takeRight 5).getOrElse("") )
    
    			import scala.io.Source
    			import spark.implicits._
    			val src = sourceHeper.readFromHDFS(csv_succursali_banche)	
    			val rdd = src.getLines().drop(1).map(f => Row.fromSeq(f.split(";"))).toList
    
    			val succ_rdd = spark.sparkContext.makeRDD(rdd).map{row =>    
        			try{
        				Row(row.getAs[String](0).toLong,
        						row.getAs[String](1),                
        						refill(row.getAs[String](2)),
        						row.getAs[String](3),
        						refill(row.getAs[String](4)),
        
        						row.getAs[String](5), 
        						row.getAs[String](6),
        						toDate(row.getAs[String](7)),
        						toDate(row.getAs[String](8)),
        						row.getAs[String](9), 
        
        						row.getAs[String](10), 
        						row.getAs[String](11), 
        						row.getAs[String](12), 
        						row.getAs[String](13),
        						row.getAs[String](14), 
        
        						row.getAs[String](15),
        						row.getAs[String](16),
        						row.getAs[String](17),
        						row.getAs[String](18).toLong,
        						row.getAs[String](19), 
        
        						row.getAs[String](20), 
        						row.getAs[String](21), 
        						toDate(row.getAs[String](22)), 
        						toDate(row.getAs[String](23)))
        		} catch {
          		  case e: Exception => {
          				null
          			}
      			}
    			}.setName("lettura del dataframe").filter(row => row != null).setName("filtro dei record non validi")
    
    			log.info("create RDD OK")
    
    
    			val succ_yarn_df = spark.createDataFrame(succ_rdd, schemaSuccursaliBanche)
    			.where("stato=86 AND des_stato LIKE 'ITALIA'")
    			.filter(row => !row.getAs[String]("cod_mecc").trim().isEmpty() || 
    			               !row.getAs[String]("cab_sede").trim().isEmpty())
    
    			log.info("createDataFrame OK")
    
    
    			succ_yarn_df.write.format(storedAs).mode(mode).save(d_succursali_banche)
    			log.info("save d_succursali_banche OK")
    
    
    			esito = "OK"
          } catch {
  			case e: Exception => {
  				throw new FormatWriteException(e.getMessage, e)
  			}
  			esito = "KO"
			}finally{
			  	log.info(s"------------------------------------------------------------------")
    			log.info(s"----                                                          ----")
    			log.info(s"----       $applicationName $esito       ----")
    			log.info(s"----                                                          ----")
    			log.info(s"------------------------------------------------------------------")
			}

	}	
}