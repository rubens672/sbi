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

class Banconota() extends Serializable{

	Logger.getLogger("org").setLevel(Level.ERROR)
	val log = org.slf4j.LoggerFactory.getLogger("it.pr.sim.ml.sintesi.Banconota")
	
	def exec()(implicit spark:SparkSession, env:Properties): DataFrame = {
	  val query = env.getProperty("SELECT_INDICATORI_BANCONOTE")
		
		log.info(s"SELECT_INDICATORI_BANCONOTE $query")
		
		val mainSubdimensionBANCONOTE = spark.sql(query)
		
		//|vnumero_pezzi| vcomb_alfa1|vnume_taglio|       vserie|  luogo_rinvenimento|vdesc_provincia
    val vcomb_alfa1_indexer = new StringIndexer().setInputCol("vcomb_alfa1").setOutputCol("vcomb_alfa1_index").setHandleInvalid("keep")
    val vnume_taglio_indexer = new StringIndexer().setInputCol("vnume_taglio").setOutputCol("vnume_taglio_index").setHandleInvalid("keep")
    val vserie_indexer = new StringIndexer().setInputCol("vserie").setOutputCol("vserie_index").setHandleInvalid("keep")
    val luogo_rinvenimento_indexer = new StringIndexer().setInputCol("luogo_rinvenimento").setOutputCol("luogo_rinvenimento_index").setHandleInvalid("keep")
    val vdesc_provincia_indexer = new StringIndexer().setInputCol("vdesc_provincia").setOutputCol("vdesc_provincia_index").setHandleInvalid("keep")
    
    
    val inputCols = Array("vcomb_alfa1_index","vnume_taglio_index","vserie_index","luogo_rinvenimento_index","vdesc_provincia_index")
    val outputCols = Array("vcomb_alfa1_vec","vnume_taglio_vec","vserie_vec","luogo_rinvenimento_vec","vdesc_provincia_vec")
    val encoder = new OneHotEncoderEstimator().setInputCols(inputCols).setOutputCols(outputCols)
		
    val assembler = new VectorAssembler().setInputCols(Array("vnumero_pezzi","vcomb_alfa1_vec","vnume_taglio_vec","vserie_vec","luogo_rinvenimento_vec","vdesc_provincia_vec")).setOutputCol("features")
    val scaler = new MinMaxScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
    
    val stages = Array(vcomb_alfa1_indexer,vnume_taglio_indexer,vserie_indexer,luogo_rinvenimento_indexer,vdesc_provincia_indexer, encoder, assembler, scaler)
    val pipeline = new Pipeline().setStages(stages)
    val pipelineModel = pipeline.fit(mainSubdimensionBANCONOTE)
    
    val subdimensionBANCONOTENorm = pipelineModel.transform(mainSubdimensionBANCONOTE)
    
    
    //---CALCULATING-SUB-INDICATOR-BANCONOTE------
    import spark.implicits._
    import org.apache.spark.sql.functions._
    val rowsBANCONOTE = subdimensionBANCONOTENorm.select("scaledFeatures")
    val avgBANCONOTE=rowsBANCONOTE
      .select("scaledFeatures")
      .map(r => r(0).asInstanceOf[org.apache.spark.ml.linalg.DenseVector].toArray.sum/r(0).asInstanceOf[org.apache.spark.ml.linalg.DenseVector].size).toDF("avg-banconote")
    
    val subIndicatorBANCONOTE= avgBANCONOTE.agg( avg(col("avg-banconote")).as("sub_indicatore_BANCONOTE") )
		
    subIndicatorBANCONOTE
	}
  
}