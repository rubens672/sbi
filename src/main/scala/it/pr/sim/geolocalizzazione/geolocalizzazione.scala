package it.pr.sim.geolocalizzazione

import scala.io._
import org.apache.commons._
import org.apache.http._
import org.apache.http.client._
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.DefaultHttpClient
import java.util.ArrayList
import org.apache.http.message.BasicNameValuePair
import org.apache.http.client.entity.UrlEncodedFormEntity

import java.util.Properties
import java.io.FileInputStream

import scala.util.Try

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark._
import org.apache.log4j.{Level, Logger}

import it.pr.sim.schema.Schemas._
import it.pr.sim.SparkJob
import it.pr.sim.exception.Exception._

import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}


/**
### Tabella di Geolocalizzazione
create table sbi.D_GEOLOCALIZZAZIONE(    
    SEQU_GEOLOCALIZZAZIONE		string,  
    NOME_TABELLA_GEOLOCALIZZATA	string,  
    FK_TABELLA_GEOLOCALIZZATA	bigint,  
    INDIRIZZO_INPUT				string,  
    COMUNE_INPUT				string,  
    SIGLA_PROVINCIA_INPUT		string,  
    REGIONE_INPUT				string,  
    INDIRIZZO_NORM				string,  
    COMUNE_NORM					string,  
    SIGLA_PROVINCIA_NORM		string,  
    CAP_NORM					string,  
    LATITUDINE	  				DOUBLE,  
    LONGITUDINE	  				DOUBLE,  
    FLAG_SUCCESSO_SI_NO			string,  
    ROW_CREATED_USER			string,  
    STATUS_LINE                 string,  
    ROW_CREATED_DTTM			TIMESTAMP )   
ROW FORMAT DELIMITED  
FIELDS TERMINATED BY '\t'  
STORED AS PARQUET  
LOCATION '/user/pr/sbi/D_GEOLOCALIZZAZIONE';
 */

object geolocalizzazione extends SparkJob("Geolocalizzazione SBI", "it.pr.sim.geolocalizzazione"){


	def run() : Unit = {
			log.info(s"----------------------------------------------------------------")
			log.info(s"----                                                        ----")
			log.info(s"----          $applicationName start...              ----")
			log.info(s"----                                                        ----")
			log.info(s"----------------------------------------------------------------")
			
      try{
        
        val geo = new GEO(env, sparkUser)
        
        geo.exec()

			  esito = "OK"
			} catch {
  			case e: Exception => {
  				log.error(e.getMessage())
  			}
  			
  			esito = "KO"
			}finally{
			    log.info(s"----------------------------------------------------------------")
    			log.info(s"----                                                        ----")
    			log.info(s"----            $applicationName $esito                  ----")
    			log.info(s"----                                                        ----")
    			log.info(s"----------------------------------------------------------------")
			}
			

	}

	
}