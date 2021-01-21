package it.pr.sim.ml.carte

import it.pr.sim.ml.SparkJob
import it.pr.sim.ml.schema.Schemas

import scala.util.Try
import scala.collection.mutable
import java.sql.Timestamp
import java.util.Calendar
import java.text.SimpleDateFormat

import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
//it.pr.sim.ml.carte.TransazioniNonRiconosciuteGraph
object TransazioniNonRiconosciuteGraph extends SparkJob("SBI-2 - Transazini Non Riconosciute"){
  
//  case class ItemProperty(val label:String, val description:String, val latitude:Double, val longitude:Double)
  
  case class ItemProperty(val label:String, val description:String, val denominazione:String)
  case class EdgeProperty(val label:String, val weight:Double, val data_transazione:java.sql.Timestamp)

  def run() : Unit = {
			log.info(s"-------------------------------------------------------------")
			log.info(s"----  $applicationName start...  ----")
			log.info(s"-------------------------------------------------------------")
			
			val query = env.getProperty("SELECT_TRANS_NON_RIC_GRAPH")
			log.info(s"SELECT_TRANS_NON_RIC_GRAPH: $query")
			
			
			val d_trans_non_ric_triplets = env.getProperty("sbi.hadoop.d_trans_non_ric_triplets")
			val storedAs = env.getProperty("sbi.hadoop.format")
			val mode = env.getProperty("sbi.hadoop.mode")
			
			log.info(s"d_trans_non_ric_triplets = ${d_trans_non_ric_triplets}")
			log.info(s"storedAs = ${storedAs}")
			log.info(s"mode = ${mode}")
			
			
			val df = spark.sql(query)
			
			/**
			 * Creazione di archi e nodi per ogni transazione non riconosciuta.
			 */
			def toHashCode(str:String): Long = {
          val a = str.toCharArray
          val l = str.length
          var i =  0
          var sum = 0L
          for(i <- 0 until l){ sum += a(i)*java.lang.Math.pow(3,l-(i+1)).toLong }
          sum
      }
			
			import spark.implicits._
			
      val listItemProperty = df.rdd.map{row =>
      try{
			  //carta
        val id_carta:Long = Try(row.getAs[Long](0)).getOrElse(0L)
        val label_carta:String = row.getAs[String](1)
        val pan_carta:String = Try(row.getAs[String](2).trim()).getOrElse("PAN NON DEFINITO")
        
        //canale (azienda o cab_atm)
        val label_canale:String = row.getAs[String](3)
        val description_canale:String = Try(row.getAs[String](4).trim()).getOrElse("CANALE NON DEFINITO")
//        val description_geo:String = Try(row.getAs[String](5).trim()).getOrElse("LOCALIZZAZIONE NON DEFINITA")
        val id_canale:Long = toHashCode(description_canale)
        
        //acquirer (atm, pos)
        val label_acquirer:String = row.getAs[String](6)
        val description_acquirer:String = Try(row.getAs[String](7).trim()).getOrElse("ACQUIRER NON DEFINITO")
        val denominazione_acquirer:String = Try(row.getAs[String](8).trim()).getOrElse(description_acquirer)
        val id_acquirer:Long = Try(toHashCode(description_acquirer)).getOrElse(1L)
        
        //issuer (codi_codice_abi_emittente_cart)
        val label_issuer:String = row.getAs[String](9)
        val description_issuer:String = Try(row.getAs[String](10).trim()).getOrElse("ISSUER NON DEFINITO")
        val denominazione_issuer:String = Try(row.getAs[String](11).trim()).getOrElse(description_issuer)
        val id_issuer:Long = Try(toHashCode(description_issuer)).getOrElse(2L)
        
        //luogo
        val label_geo:String = row.getAs[String](15)
        val description_geo:String = Try(row.getAs[String](5).trim()).getOrElse("LOCALIZZAZIONE NON DEFINITA")
        val sigla_provincia_norm:String = Try(row.getAs[String](14).trim()).getOrElse("")
        val id_luogo:Long = Try(toHashCode(description_geo)).getOrElse(1L)
        
        //valore, data
        val valore:Double = Try(row.getAs[Double](12)).getOrElse(0D)
        val data_transazione:Timestamp = Try(row.getAs[Timestamp](13)).getOrElse(null)
        
        
        //quattro nodi (carta, canale, issuer, acquirer)
        val luogo = ItemProperty(label_geo, description_geo, sigla_provincia_norm)
        val carta = ItemProperty(label_carta, pan_carta, id_carta.toString())
        val canale = ItemProperty(label_canale, description_canale, "")
        val acquirer = ItemProperty(label_acquirer, description_acquirer, denominazione_acquirer)
        val issuer = ItemProperty(label_issuer, description_issuer, denominazione_issuer)
        
        //tre archi (carta_canale, carta_acquirer, carta_issuer)
        val issuer_luogo = EdgeProperty("issuer_luogo", valore, data_transazione)
        val carta_canale = EdgeProperty("carta_canale", valore, data_transazione)
        val carta_acquirer = EdgeProperty("carta_acquirer", valore, data_transazione)
        val carta_issuer = EdgeProperty("carta_issuer", valore, data_transazione)
        
        
        //items
        val items = Array((id_carta, carta),
                          (id_canale, canale),
                          (id_acquirer, acquirer),
                          (id_issuer, issuer),
                          (id_luogo, luogo))
        
        //eges
        val edges = Array(Edge(id_carta, id_canale, carta_canale),
                          Edge(id_carta, id_acquirer, carta_acquirer),
                          Edge(id_carta, id_issuer, carta_issuer),
                          Edge(id_issuer, id_luogo, issuer_luogo))                  
                          
        
        (items, edges)                  
                          
      }catch{
        case e: Exception => 
            e.printStackTrace()
            null
      	}
      }.setName("Crea archi e nodi")
      .filter( row => row != null)
      .setName("Filtra elementi non validi")
      
      log.info("Crea archi e nodi")
      
      
      /**
       * Aggregazione di tutti i nodi e archi.
       */
      val items:RDD[(VertexId, ItemProperty)] = listItemProperty.flatMap( items => items._1)
      log.info("Crea items")  
      
      val edges:RDD[Edge[EdgeProperty]] = listItemProperty.flatMap ( items => items._2)
      log.info("Crea edges")
      
      val defaultItem = ItemProperty("defaultItem", "defaultDescription", "defaultDenominazione")
      
      
      /**
       * Creazione del grafo.
       */
      val graphIs = Graph(items, edges, defaultItem)
      log.info("Istanzia Graph")
      
      
      val graph = graphIs.cache()
      log.info("Istanza Graph cached")
      
      
      /**
       * Creazione del RDD contenenti le triplette nodo->arco->nodo elaborate da GraphX    
       */
      val tripletsRDD: RDD[Row] = graph.triplets.map({triplet => 
        val cal2 = Calendar.getInstance()
        cal2.setTime(triplet.attr.data_transazione)
        val anno_transazione:Integer = cal2.get(Calendar.YEAR)
  
        Row(triplet.srcId,
            triplet.srcAttr.label,
            triplet.srcAttr.description,
            triplet.srcAttr.denominazione,
            triplet.dstId,
            triplet.dstAttr.label,
            triplet.dstAttr.description,
            triplet.dstAttr.denominazione,
            triplet.attr.label,
            triplet.attr.weight,
            triplet.attr.data_transazione
//            ,anno_transazione
            )
      }).setName("Crea RDD tripletsRDD")
      log.info("Crea RDD tripletsRDD")
          
      
      /**
       * Calcolo delle metriche.
       */
//      val numEdges: Long = graph.numEdges
//      val numVertices: Long = graph.numVertices
//      log.info(s"numEdges: ${numEdges} - numVertices: ${numVertices}")    
      
      // Calculate centralities.  
//      log.info("Degree centrality")
//      val degrees = graph.degrees.sortByKey().map({ case (n, v) => println(s"Node: ${n} -> Degree: ${v}") })
//      
//      
//      val src = graph.triplets.map(triplet => triplet.srcId)
//      val dst = graph.triplets.map(triplet => triplet.dstId)     
//
//      val es = Array(Edge(1,1,1))
//      
//      val sparkDF = spark.sparkContext.makeRDD(es)
//      
//      val gg : Graph[Any, Int] = Graph.fromEdges(sparkDF, 1)

      
//      println("\n### Betweenness centrality\n")
//      val h: Graph[Double, Int] = Betweenness.run(gg)
//      h.vertices.sortByKey().collect().foreach { case (n, v) => println(s"Node: ${n} -> Betweenness: ${v}") }
      
      //salva triplette e metriche
      val tripletsDF = spark.createDataFrame(tripletsRDD, Schemas.trans_non_ric_triplets)
      log.info("Creazione DataFrame tripletsDF OK")
      
      //tripletsDF.show(10)
      
      tripletsDF
        .write
        .format(storedAs)
        .mode(mode)
//        .partitionBy("attr_anno_transazione")
        .save(d_trans_non_ric_triplets)
    	log.info("save d_trans_non_ric_triplets OK")
      
          
			log.info(s"-------------------------------------------------------------")
    	log.info(s"----     $applicationName OK     ----")
    	log.info(s"-------------------------------------------------------------")
  	}
}