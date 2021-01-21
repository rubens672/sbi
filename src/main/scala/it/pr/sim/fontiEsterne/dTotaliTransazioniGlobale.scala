package it.pr.sim.fontiEsterne

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import java.text.SimpleDateFormat
import java.sql.Timestamp
import org.apache.spark.sql.Row
import org.apache.log4j.{Level, Logger}

import it.pr.sim.schema.Schemas._
import it.pr.sim.SparkJob
import it.pr.sim.exception.Exception._

import scala.util.Try

/**
## D_TOTALE_TRANSAZIONI_GLOBALE
CREATE TABLE sbi.D_TOTALE_TRANSAZIONI_GLOBALE(  
    anno_riferimento int,  
    tipo_dato string,  
    numero_transazioni bigint,  
    valore_transazioni bigint  
  )  
ROW FORMAT DELIMITED  
FIELDS TERMINATED BY '\t'  
STORED AS PARQUET  
LOCATION '/user/pr/sbi/D_TOTALE_TRANSAZIONI_GLOBALE';
 */

object dTotaliTransazioniGlobale extends SparkJob("Fonti Esterne SBI - D_TOTALE_TRANSAZIONI_GLOBALE", "it.pr.sim.fontiEsterne.dTotaliTransazioniGlobale") {
	
		def run() : Unit = {
			log.info(s"------------------------------------------------------------------")
			log.info(s"----                                                          ----")
			log.info(s"--  $applicationName start... --")
			log.info(s"----                                                          ----")
			log.info(s"------------------------------------------------------------------")

      try{	
    		val dataelaborazione = new java.sql.Timestamp(System.currentTimeMillis())
    		val storedAs = env.getProperty("sbi.fonti.esterne.hadoop.format")
    		val mode = env.getProperty("sbi.fonti.esterne.hadoop.mode")
    		val csv_num_trans_cart_cred = repoPath.concat(env.getProperty("sbi.fonti.esterne.csv.num_trans_cart_cred"))
    		val csv_val_trans_cart_cred = repoPath.concat(env.getProperty("sbi.fonti.esterne.csv.val_trans_cart_cred"))
    		val csv_num_trans_cart_deb = repoPath.concat(env.getProperty("sbi.fonti.esterne.csv.num_trans_cart_deb"))
    		val csv_val_trans_cart_deb = repoPath.concat(env.getProperty("sbi.fonti.esterne.csv.val_trans_cart_deb"))
    		val d_totale_tranzazioni_globale = env.getProperty("sbi.fonti.esterne.hadoop.d_totale_tranzazioni_globale")
  
    
    		log.info("sbi.fonti.esterne.csv.num_trans_cart_cred: " + csv_num_trans_cart_cred)
    		log.info("sbi.fonti.esterne.csv.val_trans_cart_cred: " + csv_val_trans_cart_cred)
    		log.info("sbi.fonti.esterne.csv.num_trans_cart_deb: " + csv_num_trans_cart_deb)
    		log.info("sbi.fonti.esterne.csv.val_trans_cart_deb: " + csv_val_trans_cart_deb)
        log.info("sbi.fonti.esterne.hadoop.d_totale_tranzazioni_globale: " + d_totale_tranzazioni_globale)
        log.info("sbi.fonti.esterne.hadoop.format: " + storedAs)
        log.info("sbi.fonti.esterne.hadoop.mode: " + mode)
  			
        
        val toAnnoUDF = udf((date:String) => Try(date.split("/")(2).toInt).getOrElse(0))
        val tdUDF = udf((td:String) =>  td.split(":")(0) )
        val kUDF = udf((k:Long) => (k * 1000))
        val MUDF = udf((M:Long) => (M * 1000000))
  			
        
        import spark.implicits._
        
        val numStruct = new StructType(Array(StructField("anno_riferimento",StringType,nullable = true),StructField("numero_transazioni",StringType,nullable = true)))
        val valStruct = new StructType(Array(StructField("anno_riferimento",StringType,nullable = true),StructField("valore_transazioni",StringType,nullable = true)))
        
        /* caricamento carte di credito */
  			val row1 =sourceHeper.readFromHDFS(csv_num_trans_cart_cred).getLines().drop(1).map(f => Row.fromSeq(f.split(";"))).toList
  			val rdd1 = spark.sparkContext.makeRDD(row1).map{row =>      
            Try(Row( row.getAs[String](3), row.getAs[String](5))).getOrElse(null)
    			}.filter(row => row != null)
  			log.info("create RDD1 OK")
  			
  			val num_trans_cart_cred_df = spark.createDataFrame(rdd1, numStruct)
  			      .select(toAnnoUDF(col("anno_riferimento")).alias("anno_riferimento"),
                      lit("CREDITO").alias("tipo_dato_num"),
                      kUDF(col("numero_transazioni").cast(LongType)).alias("numero_transazioni"))
              .groupBy("anno_riferimento","tipo_dato_num")
              .agg(sum("numero_transazioni").alias("numero_transazioni"))
        log.info("read num_trans_cart_cred_df") 
        
        
        val row2 = sourceHeper.readFromHDFS(csv_val_trans_cart_cred).getLines().drop(1).map(f => Row.fromSeq(f.split(";"))).toList
  			val rdd2 = spark.sparkContext.makeRDD(row2).map{row =>      
            Try(Row( row.getAs[String](3), row.getAs[String](5))).getOrElse(null)
    			}.setName("lettura del dataframe").filter(row => row != null).setName("filtro dei record non validi")
  			log.info("create RDD2 OK")
  			
  			val val_trans_cart_cred_df = spark.createDataFrame(rdd2, valStruct)
  			      .select(toAnnoUDF(col("anno_riferimento")).alias("anno_riferimento"),
                      lit("CREDITO").alias("tipo_dato"),
                      MUDF(col("valore_transazioni").cast(LongType)).alias("valore_transazioni"))
              .groupBy("anno_riferimento","tipo_dato")
              .agg(sum("valore_transazioni").alias("valore_transazioni"))
        log.info("read val_trans_cart_cred_df")
  			
    
                    
        val join_cart_cred_df = num_trans_cart_cred_df.join(val_trans_cart_cred_df, "anno_riferimento")
        log.info("join_cart_cred_df")
                        
        val trans_cart_cred_df = join_cart_cred_df
                        .select("anno_riferimento","tipo_dato","numero_transazioni","valore_transazioni")
                        .orderBy(desc("anno_riferimento")) 
        log.info("read trans_cart_cred_df")                
                      
        
        /* caricamento carte di debito */
        val row3 = sourceHeper.readFromHDFS(csv_num_trans_cart_deb).getLines().drop(1).map(f => Row.fromSeq(f.split(";"))).toList
  			val rdd3 = spark.sparkContext.makeRDD(row3).map{row =>      
            Try(Row( row.getAs[String](2), row.getAs[String](4))).getOrElse(null)
    			}.filter(row => row != null)
  			log.info("create RDD3 OK")
  			
  			val num_trans_cart_deb_df = spark.createDataFrame(rdd3, numStruct)
  			      .select(toAnnoUDF(col("anno_riferimento")).alias("anno_riferimento_num"),
                      lit("DEBITO").alias("tipo_dato_num"),
                      kUDF(col("numero_transazioni").cast(LongType)).alias("numero_transazioni"))
              .groupBy("anno_riferimento_num","tipo_dato_num")
              .agg(sum("numero_transazioni").alias("numero_transazioni"))
        log.info("read num_trans_cart_deb_df")
        
        
        
        val row4 = sourceHeper.readFromHDFS(csv_val_trans_cart_deb).getLines().drop(1).map(f => Row.fromSeq(f.split(";"))).toList
  			val rdd4 = spark.sparkContext.makeRDD(row4).map{row =>      
            Try(Row( row.getAs[String](2), row.getAs[String](4))).getOrElse(null)
    			}.filter(row => row != null)
  			log.info("create RDD4 OK")
  			
  			val val_trans_cart_deb_df = spark.createDataFrame(rdd4, valStruct)
  			      .select(toAnnoUDF(col("anno_riferimento")).alias("anno_riferimento"),
                      lit("DEBITO").alias("tipo_dato"),
                      MUDF(col("valore_transazioni").cast(LongType)).alias("valore_transazioni"))
              .groupBy("anno_riferimento","tipo_dato")
              .agg(sum("valore_transazioni").alias("valore_transazioni"))
        log.info("read val_trans_cart_deb_df")
        
        
        val groupNumTrans = num_trans_cart_deb_df.groupBy("anno_riferimento_num","tipo_dato_num").agg(sum("numero_transazioni").alias("numero_transazioni"))            
        val groupValTrans = val_trans_cart_deb_df.groupBy("anno_riferimento","tipo_dato").agg(sum("valore_transazioni").alias("valore_transazioni"))
        
        val join_cart_deb_df = groupNumTrans.join(groupValTrans, groupNumTrans("anno_riferimento_num") === groupValTrans("anno_riferimento"))  
        log.info("join_cart_deb_df ok")
                                           
        val trans_cart_deb_df = join_cart_deb_df
                      .select("anno_riferimento","tipo_dato","numero_transazioni","valore_transazioni")
                      .orderBy(desc("anno_riferimento"))
        
        log.info("join ok")
        
                      
        /* crea i dataframe*/
        val cred_df = spark.createDataFrame(trans_cart_cred_df.rdd, schemaTotaliTransazioniGlobale)   
        val deb_df = spark.createDataFrame(trans_cart_deb_df.rdd, schemaTotaliTransazioniGlobale)
        log.info("createDataFrame OK")  
        
        /* unisce i due dataframe */
        val df = cred_df.union(deb_df).orderBy(desc("anno_riferimento"))
        log.info("union cred_df to deb_df")  
        
        /* salva su hadoop*/         
        df.write.format(storedAs).mode(mode).save(d_totale_tranzazioni_globale)              
        log.info("save d_totale_transazioni_globale OK")
        
        
        esito = "OK"
      } catch {
  			case e: Exception => {
  				throw new FormatWriteException(e.getMessage, e)
  			}
  			esito = "KO"
			}finally{
			    log.info(s"------------------------------------------------------------------")
      		log.info(s"----                                                          ----")
      		log.info(s"----  $applicationName $esito   ----")
      		log.info(s"----                                                          ----")
      		log.info(s"------------------------------------------------------------------")
			}
	}
	
}