package it.pr.sim.fontiEsterne

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
//import org.apache.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.text.SimpleDateFormat
import java.sql.Timestamp
//import org.apache.spark.sql.Row
import org.apache.log4j.{Level, Logger}

import it.pr.sim.schema.Schemas._
import it.pr.sim.SparkJob
import it.pr.sim.exception.Exception._

//import org.apache.poi.ss.usermodel.{ DataFormatter, WorkbookFactory, Row }
import java.io.File
import collection.JavaConversions._ // lets you iterate over a java iterable
import scala.util.Try

/**
## D_MERCHANT_CATEG_GROUP
CREATE TABLE sbi.D_MERCHANT_CATEG_GROUP(  
    codice_mcc string,  
    descrizione_mcc string,  
    codice_mcg bigint,  
    descrizione_mcg string  
  )  
ROW FORMAT DELIMITED  
FIELDS TERMINATED BY '\t'  
STORED AS PARQUET  
LOCATION '/user/pr/sbi/D_MERCHANT_CATEG_GROUP';
 */

object dMerchantCategGroup extends SparkJob("Fonti Esterne SBI - D_MERCHANT_CATEG_GROUP", "it.pr.sim.fontiEsterne.dMerchantCategGroup") {

	def run() : Unit = {
			log.info(s"------------------------------------------------------------------")
			log.info(s"----                                                          ----")
			log.info(s"----   $applicationName start...  ----")
			log.info(s"----                                                          ----")
			log.info(s"------------------------------------------------------------------")  
			
      
      try{			
    			val row_created_user = sparkUser
    			val dataelaborazione = new java.sql.Timestamp(System.currentTimeMillis())
    			val storedAs = env.getProperty("sbi.fonti.esterne.hadoop.format")
    			val mode = env.getProperty("sbi.fonti.esterne.hadoop.mode")
    			val csv_mccmcg_visaeurope = repoPath.concat(env.getProperty("sbi.fonti.esterne.csv.mccmcg_visaeurope"))
    			val d_merchant_categ_group = env.getProperty("sbi.fonti.esterne.hadoop.d_merchant_categ_group")
    
    			log.info("sbi.fonti.esterne.csv.mccmcg_visaeurope: " + csv_mccmcg_visaeurope)
          log.info("sbi.fonti.esterne.hadoop.d_merchant_categ_group: " + d_merchant_categ_group)
          log.info("sbi.fonti.esterne.hadoop.format: " + storedAs)
          log.info("sbi.fonti.esterne.hadoop.mode: " + mode)
    			
            
    			val src = sourceHeper.readFromHDFS(csv_mccmcg_visaeurope) 
    			val row = src.getLines().drop(1).map(f => org.apache.spark.sql.Row.fromSeq(f.split(";"))).toList
    			val sparkDF = spark.sparkContext.makeRDD(row).setName("creazione RDD (makeRDD)")
            
          val refill:String => String = ( f => ("0000".concat(f)  takeRight 4) )
            
          val rdd = sparkDF.map{row =>      
            try{
              org.apache.spark.sql.Row(
                  refill(row.getAs[String](0)),
                  row.getAs[String](1),
                  row.getAs[String](2),
                  row.getAs[String](3) 
                 )
            } catch {
            		  case e: Exception => {
            				null
            			}
        			}
      			}.setName("lettura del dataframe").filter(row => row != null).setName("filtro dei record non validi")
    					
    			log.info("create RDD OK")
    
    			
    			val df = spark.createDataFrame(rdd, schemaMerchantCategGroup)
    			log.info("createDataFrame OK")
    			
    			df.write.format(storedAs).mode(mode).save(d_merchant_categ_group)
    			log.info("save d_merchant_categ_group OK")
    			
    			
    			esito = "OK"
      } catch {
  			case e: Exception => {
  				throw new FormatWriteException(e.getMessage, e)
  			}
  			esito = "KO"
			}finally{
			  log.info(s"------------------------------------------------------------------")
  			log.info(s"----                                                          ----")
  			log.info(s"----      $applicationName $esito     ----")
  			log.info(s"----                                                          ----")
  			log.info(s"------------------------------------------------------------------")
			}
			
	}
	
//	def getCsv(file:String) : List[org.apache.spark.sql.Row] = {
//	  val f = new File(file)
//    val workbook = WorkbookFactory.create(f)
//    val sheet = workbook.getSheetAt(0) // Assuming they're in the first sheet here.
//    val formatter = new DataFormatter()
//    
//    val rdd:List[org.apache.spark.sql.Row] = sheet.map { row => 
//       val rowErr = Try({
//         val maybeA = formatter.formatCellValue(row.getCell(0, Row.MissingCellPolicy.RETURN_NULL_AND_BLANK)).trim().toLong 
//         val maybeB = formatter.formatCellValue(row.getCell(1, Row.MissingCellPolicy.RETURN_NULL_AND_BLANK)).trim()
//         val maybeC = formatter.formatCellValue(row.getCell(2, Row.MissingCellPolicy.RETURN_NULL_AND_BLANK)).trim().toLong 
//         val maybeD = formatter.formatCellValue(row.getCell(3, Row.MissingCellPolicy.RETURN_NULL_AND_BLANK)).trim()
//         
//         org.apache.spark.sql.Row(maybeA, maybeB, maybeC, maybeD)
//       }).getOrElse(null)
//     
//       workbook.close()
//       
//      rowErr
//    }.filter(f => f != null).toList
//    
//    rdd
//	}
}