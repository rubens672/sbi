package it.pr.sim.ml.carte

import it.pr.sim.ml.SparkJob
import it.pr.sim.ml.schema.Schemas

import org.apache.spark.ml.feature.{MinMaxScaler, VectorAssembler}
import org.apache.spark.ml.linalg.{Matrix, Vectors}
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.Row

import scala.util.Try

/**
 * REF001.3 - Analisi di correlazione multipla
 * 
 * Il presente requisito si occupa di eseguire un'analisi di correlazione per consentire la comprensione di un nesso di linearità
 * tra le variabili statistiche caratterizzanti il fenomeno.
 * 
 * Dato il dataset da analizzare, una volta avviato, il processo esegue l'analisi di correlazione multipla producendo in output 
 * un nuovo dataset comprensivo dei risultati di questa elaborazione.
 */

object AnalisiCorrelazioneMultipla extends SparkJob("SBI-2 - Analisi Correlazione Multipla"){
  
  def run() : Unit = {
    log.info(s"---------------------------------------------------------------")
		log.info(s"----  $applicationName start...  ----")
		log.info(s"---------------------------------------------------------------") 
			
		val query = env.getProperty("SELECT_TRANS_NON_RIC_CORR")
		val storedAs = env.getProperty("sbi.hadoop.format")
		val mode = env.getProperty("sbi.hadoop.mode")
		val d_correlazione_multipla = env.getProperty("sbi.hadoop.d_correlazione_multipla")
			
		log.info(s"SELECT_TRANS_NON_RIC_CORR: $query")
		log.info(s"storedAs: $storedAs")
		log.info(s"mode: $mode")
		log.info(s"d_correlazione_multipla: $d_correlazione_multipla")

		
    import spark.implicits._

    /** creazione dataset da query **/
		val df = spark.sql(query)
    log.info("creazione dataset da query")
    
    log.info("cols name: ", df.schema.names.mkString(","))
    
    
    /** preprocessing array caratteristiche **/
    val assembler = new VectorAssembler()
      .setInputCols(df.schema.names)
      .setOutputCol("features")
      .setHandleInvalid("keep")
      
    val output = assembler.transform(df)
    log.info("VectorAssembler transform")
    
    output.show()
		
    /** riduzione di scala minmax **/
    val scaler = new MinMaxScaler()
        .setInputCol("features")
        .setOutputCol("scaledFeatures")
        
    // Compute summary statistics and generate MinMaxScalerModel
    val scalerModel = scaler.fit(output)
    log.info("fit MinMaxScaler")
    
    // rescale each feature to range [min, max].
    val scaledData = scalerModel.transform(output)
    log.info("generate MinMaxScaler")
    
    
    /** calcolo delle correlazioni **/
    val Row(coeff1: Matrix) = Correlation.corr(scaledData, "scaledFeatures").head
    println(s"Pearson correlation matrix:\n $coeff1")
    log.info("calcolo delle correlazioni")
    
    
    /** creazione del dataset per visualizzazione su redash **/
    val round:Double => Double = ( BigDecimal( _ ).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble )
    val cols = df.schema.names
    val cols_num = df.schema.names.length - 1
    val matrix = (0 to cols_num).map{r =>
        val subMatrix = (0 to cols_num).map{ c =>
           log.info(cols(r)+" "+cols(c)+" "+ Try(round(coeff1.apply(r,c))).getOrElse(0.0D) )
           Row(cols(r), cols(c), Try(round(coeff1.apply(r,c))).getOrElse(0.0D) )
        }
        subMatrix
    }.flatMap(f => f).toList
    log.info("creazione del dataset per visualizzazione su redash")
    
    
    /** creazione dataframe **/
    val dfX = spark.sparkContext.makeRDD(matrix).setName("creazione dataframe")
    log.info("creazione rdd")
    
    val dfZ = spark.createDataFrame(dfX, Schemas.correlazione_multipla)
    log.info("creazione dataframe")
    
    dfZ.show()
    
    /** salvataggio su hive **/
    dfZ.write.format(storedAs).mode(mode).save(d_correlazione_multipla)
    log.info("save d_correlazione_multipla OK")
    
    
    log.info(s"---------------------------------------------------------------")
		log.info(s"----     $applicationName OK     ----")
		log.info(s"---------------------------------------------------------------")
  }
}