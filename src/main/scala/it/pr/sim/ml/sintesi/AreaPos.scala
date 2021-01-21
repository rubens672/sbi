package it.pr.sim.ml.sintesi

import java.util.Properties

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark._
import org.apache.log4j.{Level, Logger}

import org.apache.spark.sql.functions._
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{StringIndexer, OneHotEncoderEstimator, VectorAssembler, MinMaxScaler}

import it.pr.sim.ml.schema.Schemas

class AreaPos() extends Serializable{

	Logger.getLogger("org").setLevel(Level.ERROR)
	val log = org.slf4j.LoggerFactory.getLogger("it.pr.sim.ml.sintesi.AreaPos")
	
	def exec()(implicit spark:SparkSession, env:Properties): DataFrame = {
	  val query = env.getProperty("SELECT_INDICATORI_POS")
		
		log.info(s"SELECT_INDICATORI_POS $query")
		
		val mainSubdimensionPOS = spark.sql(query)
		
		//codi_codice_abi_emittente_cart|codi_codice_pan_carta|impo_importo_transazione|desc_motivo_disconoscimento| desc_cat_merc|desc_canale|desc_funzionalita_carta
    val codi_codice_abi_emittente_cart_indexer = new StringIndexer().setInputCol("codi_codice_abi_emittente_cart").setOutputCol("codi_codice_abi_emittente_cart_index").setHandleInvalid("keep")
    val codi_codice_pan_carta_indexer = new StringIndexer().setInputCol("codi_codice_pan_carta").setOutputCol("codi_codice_pan_carta_index").setHandleInvalid("keep")
    val desc_motivo_disconoscimento_indexer = new StringIndexer().setInputCol("desc_motivo_disconoscimento").setOutputCol("desc_motivo_disconoscimento_index").setHandleInvalid("keep")
    val desc_cat_merc_indexer = new StringIndexer().setInputCol("desc_cat_merc").setOutputCol("desc_cat_merc_index").setHandleInvalid("keep")
    val desc_canale_indexer = new StringIndexer().setInputCol("desc_canale").setOutputCol("desc_canale_index").setHandleInvalid("keep")
    val desc_funzionalita_carta_indexer = new StringIndexer().setInputCol("desc_funzionalita_carta").setOutputCol("desc_funzionalita_carta_index").setHandleInvalid("keep")
    
    val inputCols = Array("codi_codice_abi_emittente_cart_index","codi_codice_pan_carta_index","desc_motivo_disconoscimento_index","desc_cat_merc_index","desc_canale_index","desc_funzionalita_carta_index")
    val outputCols = Array("codi_codice_abi_emittente_cart_vec", "codi_codice_pan_carta_vec", "desc_motivo_disconoscimento_vec", "desc_cat_merc_vec", "desc_canale_vec", "desc_funzionalita_carta_vec")
    val encoder = new OneHotEncoderEstimator().setInputCols(inputCols).setOutputCols(outputCols)
    
    val assembler = new VectorAssembler().setInputCols(Array("codi_codice_abi_emittente_cart_vec", "codi_codice_pan_carta_vec" ,"impo_importo_transazione", "desc_motivo_disconoscimento_vec", "desc_cat_merc_vec", "desc_canale_vec", "desc_funzionalita_carta_vec")).setOutputCol("features")
    val scaler = new MinMaxScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
    
    val stages = Array(codi_codice_abi_emittente_cart_indexer, codi_codice_pan_carta_indexer, desc_motivo_disconoscimento_indexer, desc_cat_merc_indexer, desc_canale_indexer, desc_funzionalita_carta_indexer, encoder, assembler, scaler)
    val pipeline = new Pipeline().setStages(stages)
    val pipelineModel = pipeline.fit(mainSubdimensionPOS)
    
    val subdimensionPOSNorm = pipelineModel.transform(mainSubdimensionPOS)
    
    
    //---CALCULATING-SUB-INDICATOR-POS------
    import spark.implicits._
    val rowsPOS = subdimensionPOSNorm.select("scaledFeatures")
    val avgPOS = rowsPOS
      .select("scaledFeatures")
      .map(r => r(0).asInstanceOf[org.apache.spark.ml.linalg.DenseVector].toArray.sum/r(0).asInstanceOf[org.apache.spark.ml.linalg.DenseVector].size).toDF("avg-pos")
    
    val subIndicatorPOS = avgPOS.agg( avg(col("avg-pos")).as("sub_indicatore_POS") )
		
    subIndicatorPOS
	}  
}