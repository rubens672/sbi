package it.pr.sim.geolocalizzazione

import scala.io._
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import org.apache.commons._
import org.apache.http._
import org.apache.http.client._
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.DefaultHttpClient
import java.util.ArrayList
import org.apache.http.message.BasicNameValuePair
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.conn.params.ConnRoutePNames

import java.util.Properties
import java.io.FileInputStream

import scala.util.Try

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark._
import org.apache.log4j.{Level, Logger}

import it.pr.sim.schema.Schemas._
import it.pr.sim.exception.Exception._

import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}


class GEO(env:Properties, sparkUser:String) extends Serializable{

	Logger.getLogger("org").setLevel(Level.ERROR)
	val log = org.slf4j.LoggerFactory.getLogger("it.pr.sim.geolocalizzazione")

	def exec()(implicit spark:SparkSession): Unit = {

			val queryProp = env.getProperty("sbi.scripts.query")

			val tabelleDaGeolocalizzare = env.getProperty("sbi.geolocalizzazione.tabelle.da.geolocalizzare").split(",")
			val d_geolocalizzazione = env.getProperty("sbi.geolocalizzazione.hadoop.d_geolocalizzazione")
			val API_ENDPOINT = env.getProperty("sbi.geolocalizzazione.api.endpoint")
			val API_KEY = env.getProperty("sbi.geolocalizzazione.api.key")
			val row_created_user = sparkUser
			val storedAs = env.getProperty("sbi.geolocalizzazione.hadoop.format")
			val proxyUrlProp = Try(env.getProperty("sbi.geolocalizzazione.proxy.url").trim()).getOrElse(null)
			val proxy = getProxy(proxyUrlProp)
			val proxyProtocol = proxy._1
			val proxyUrl = proxy._2
			val proxyPort = proxy._3

			log.info("sbi.geolocalizzazione.api.endpoint: " + API_ENDPOINT)
			log.info("sbi.geolocalizzazione.tabelle.da.geolocalizzare: " + env.getProperty("sbi.geolocalizzazione.tabelle.da.geolocalizzare"))
			log.info("sbi.geolocalizzazione.hadoop.d_geolocalizzazione: " + d_geolocalizzazione)
			log.info("sbi.geolocalizzazione.proxy.url: " + proxyUrlProp)
			log.info("sbi.fonti.esterne.hadoop.format: " + storedAs)
			log.info("proxyProtocol: " + proxyProtocol)
			log.info("proxyUrl: " + proxyUrl)
			log.info("proxyPort: " + proxyPort)


			val ind_map = tabelleDaGeolocalizzare.map{tabella =>
    			val query = env.getProperty(tabella)
    			log.info("query: " + query);
    
    			val df = spark.sql(query)
					log.info("spark.sql OK")

					val ind_map = df.rdd.repartition(50).map(line => sp(line, row_created_user, API_ENDPOINT, API_KEY, proxyProtocol, proxyUrl, proxyPort)).setName(s"Interrogazione GeoPoi tabella $tabella")
					log.info(s"interrogazione GeoPoi tabella $tabella OK")

					val geo_df = spark.createDataFrame(ind_map, schemaGeolocalizzazione)
					log.info(s"createDataFrame tabella $tabella OK")

					geo_df
			}.reduce(_.union(_)).coalesce(1)
			
			
			
  		ind_map.write.format(storedAs).mode("append").save(d_geolocalizzazione)
  		log.info(s"DataFrame write  OK")
  
  		log.info(s"geolocalizzazione OK")

			
	}
	
  	def getProxy(proxyUrl:String):(String, String, Integer) = {
      val url = if(proxyUrl != null && proxyUrl.trim().length() > 0){
        val urlArr = proxyUrl.replaceAll("/", "").split(":")
        val protocol = urlArr(0)
        val url = urlArr(1)
        val port = try{
          Integer.valueOf(urlArr(2))
        } catch {
    			case e: Exception => {
    			  throw new Exception("Porta proxy non valida: " + e.getMessage())
    			}
        }
        (protocol, url, port)
      }else{
        (null, null, null)
      }
  	  return url
  	}

  import java.nio.charset.Charset

	def sp(row:Row, row_created_user:String, API_ENDPOINT:String, API_KEY:String, proxyProtocol:String, proxyUrl:String, proxyPort:Integer): Row = {
			System.setProperty("jsse.enableSNIExtension","false")
			System.setProperty("com.sun.security.enableAIAcaIssuers","true")
      
			val fk_tabella_geolocalizzata:Long = row.getAs[Long]("sequ_tabella")
			val nome_tabella_geolocalizzata:String = row.getAs[String]("nome_tabella_geolocalizzata")
			val indirizzo_input:String = row.getAs[String]("indi_indirizzo")
			val ind_input_dpa = Try(row.getAs[String]("indi_indirizzo").trim.replaceAll(" +", " ").split(" ")).getOrElse(Array(""))
			val comune_input:String = row.getAs[String]("desc_comune")
			val sigla_provincia_input:String = row.getAs[String]("desc_sigla_provincia")
			val regione_input:String = row.getAs[String]("desc_nome_regione")
			val sequ_geolocalizz = fk_tabella_geolocalizzata + "_" + nome_tabella_geolocalizzata
			val dataelaborazione = new java.sql.Timestamp(System.currentTimeMillis())

			try{
				if(indirizzo_input.trim.equals("INTERNET")) throw new Exception("indi_indirizzo=INTERNET")

				val INDIRIZZO = if(nome_tabella_geolocalizzata.trim.equals("D_TRANSAZIONI_NON_RICONOSCIUTE")){
					Try(";" + comune_input.split(" ")(0) + ";;").getOrElse(";;")
				}else if(!nome_tabella_geolocalizzata.trim.equals("D_PUNTO_ACCETTAZIONE")){
					s"$sigla_provincia_input;$comune_input;$indirizzo_input;"
				}else if(ind_input_dpa.length == 1 || ind_input_dpa.length == 3){
					";" + ind_input_dpa(0) + ";;"
				}else if(ind_input_dpa.length > 4){
					val loc = ind_input_dpa(ind_input_dpa.length-3)
							val ind = ind_input_dpa.slice(0, ind_input_dpa.length-3).map(f => f + " ").mkString.trim
							";" + loc + ";" + ind + ";"
				}else{
					";" + ind_input_dpa.map(f => f + " ").mkString.trim + ";;"
				}
				
				val client = new DefaultHttpClient
						val nameValuePairs = new ArrayList[NameValuePair]()
						nameValuePairs.add(new BasicNameValuePair("key", API_KEY))
						nameValuePairs.add(new BasicNameValuePair("data", INDIRIZZO))   
						val post = new HttpPost(API_ENDPOINT)
						post.setEntity(new UrlEncodedFormEntity(nameValuePairs))
						
						if(proxyUrl != null){
						  val proxy = new HttpHost(proxyUrl, proxyPort, proxyProtocol)
						  client.getParams().setParameter(ConnRoutePNames.DEFAULT_PROXY, proxy)
						}


						val response = client.execute(post)
						val statusLine = response.getStatusLine().toString
						val entity = response.getEntity()
						val inputStream = entity.getContent()

						val rowOut = if(entity != null) {
						  val in:String = Source.fromInputStream(inputStream,"UTF-8").getLines.mkString
						                                                                .replaceAll("\u00c3\u00a0","a'")
						                                                                .replaceAll("\u00c3\u00a8","e'")
						                                                                .replaceAll("\u00c3\u00a9","e'")
						                                                                .replaceAll("\u00c3\u00ac","i'")
						                                                                .replaceAll("\u00c3\u00b2","o'")
						                                                                .replaceAll("\u00c3\u00b9","u'")
						  
							val splitArr = in.split(";")

									val rowOut = if(splitArr.length > 3){
										val rowOut = Row(
												sequ_geolocalizz,
												nome_tabella_geolocalizzata,
												fk_tabella_geolocalizzata,
												indirizzo_input,
												comune_input,
												sigla_provincia_input,
												regione_input,
												splitArr(5), //indirizzo_norm
												splitArr(4), //comune_norm
												splitArr(3), //sigla_provincia_norm
												splitArr(6), //cap_norm
												Try(splitArr(8).toDouble).getOrElse(0.0), //latitudine,
												Try(splitArr(7).toDouble).getOrElse(0.0), //longitudine,
												"S", //flag_successo_si_no,
												row_created_user,
												statusLine,
												dataelaborazione)
												rowOut
									}else{
										inputStream.close
										throw new Exception(statusLine)
									}

							inputStream.close
							rowOut
						}else{
							log.error("Nessuna risposta dal servizio Geopoi!!")
							inputStream.close
							throw new Exception(statusLine)
						}
				client.getConnectionManager().shutdown()
				rowOut
			} catch {
  			case e: Exception => {
  				log.error(e.getMessage())
  				val rowErr = Row(
  						sequ_geolocalizz,
  						nome_tabella_geolocalizzata,
  						fk_tabella_geolocalizzata,
  						indirizzo_input,
  						comune_input,
  						sigla_provincia_input,
  						regione_input,
  						"-", //indirizzo_norm
  						"-", //comune_norm
  						"-", //sigla_provincia_norm
  						"-", //cap_norm
  						0.0, //latitudine,
  						0.0, //longitudine,
  						"N", //flag_successo_si_no,
  						row_created_user,
  						e.getMessage(),
  						dataelaborazione)
  				rowErr
  			}
			}
	}


}