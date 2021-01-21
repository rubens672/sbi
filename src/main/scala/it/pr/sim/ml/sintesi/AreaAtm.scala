package it.pr.sim.ml.sintesi

import java.util.Properties

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark._
import org.apache.log4j.{Level, Logger}

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{StringIndexer, OneHotEncoderEstimator, VectorAssembler}
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.mllib.linalg._
import org.apache.spark.ml.linalg.{Vector, Vectors, Matrix}
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.Row

import it.pr.sim.ml.schema.Schemas

class AreaAtm() extends Serializable{

	Logger.getLogger("org").setLevel(Level.ERROR)
	val log = org.slf4j.LoggerFactory.getLogger("it.pr.sim.ml.sintesi.AreaAtm")
	
	def exec()(implicit spark:SparkSession, env:Properties): DataFrame = {
	  val query = env.getProperty("SELECT_TRANS_NON_RIC_GRAPH_ATM")
		
		log.info(s"query $query")
		
		val df = spark.sql(query)
		
//		val codi_codice_pan_carta_indexer = new StringIndexer().setInputCol("codi_codice_pan_carta").setOutputCol("codi_codice_pan_carta_index").setHandleInvalid("keep")
    val desc_codice_issuer_indexer = new StringIndexer().setInputCol("desc_codice_issuer").setOutputCol("desc_codice_issuer_index").setHandleInvalid("keep")
    val desc_motivo_disconoscimento_indexer = new StringIndexer().setInputCol("desc_motivo_disconoscimento").setOutputCol("desc_motivo_disconoscimento_index").setHandleInvalid("keep")
    val desc_funzionalita_carta_indexer = new StringIndexer().setInputCol("desc_funzionalita_carta").setOutputCol("desc_funzionalita_carta_index").setHandleInvalid("keep")
    
    val inputCols = Array( "desc_codice_issuer_index", "desc_motivo_disconoscimento_index", "desc_funzionalita_carta_index")
    val outputCols = Array( "desc_codice_issuer_vec",   "desc_motivo_disconoscimento_vec",   "desc_funzionalita_carta_vec")
    val encoder = new OneHotEncoderEstimator().setInputCols(inputCols).setOutputCols(outputCols)
    
    val assembler = new VectorAssembler().setInputCols(Array("desc_codice_issuer_vec", "desc_motivo_disconoscimento_vec", "desc_funzionalita_carta_vec")).setOutputCol("features")
    
    val stages = Array(desc_codice_issuer_indexer, desc_motivo_disconoscimento_indexer, desc_funzionalita_carta_indexer, encoder, assembler)
    val pipeline = new Pipeline().setStages(stages)
    
    val pipelineModel = pipeline.fit(df)
    val pipelineDataDF = pipelineModel.transform(df)
    
    
		val Row(coeff1: Matrix) = Correlation.corr(pipelineDataDF, "features").head
		
		import scala.util.Try
    val round:Double => Double = ( BigDecimal( _ ).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble )
    val cols = df.schema.names
    val cols_num = df.schema.names.length - 1
    val matrix = (0 to cols_num).map{r =>
        val subMatrix = (0 to cols_num).map{ c =>
           println(cols(r)+" "+cols(c)+" "+ Try(round(coeff1.apply(r,c))).getOrElse(0.0D) +" "+  "ATM" )
           Row(cols(r), cols(c), Try(round(coeff1.apply(r,c))).getOrElse(0.0D), "ATM")
        }
        subMatrix
      }.flatMap(f => f).toList
     log.info("creazione del dataset per visualizzazione su redash")
      
      
     val dfX = spark.sparkContext.makeRDD(matrix).setName("creazione dataframe")
     log.info("creazione rdd")
     
     val dfZ = spark.createDataFrame(dfX, Schemas.indicatori_sintesi)
     log.info("creazione dataframe")

     
     dfZ
  	}
  
	
}