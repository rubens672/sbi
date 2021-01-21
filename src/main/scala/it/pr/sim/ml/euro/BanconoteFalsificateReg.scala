package it.pr.sim.ml.euro

import it.pr.sim.ml.SparkJob
import it.pr.sim.ml.schema.Schemas

import scala.util.Try

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{StringIndexer, OneHotEncoderEstimator, VectorAssembler, MinMaxScaler}
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.ml.evaluation.{RegressionEvaluator}
import org.apache.spark.mllib.evaluation.RegressionMetrics

/**
 * REF002.1 Banconote falsificate  -  Analisi di Regressione
 * 
 * Il presente requisito si occupa di predire l'evoluzione e la diffusione delle banconote falsificate sul territorio nazionale, 
 * a partire dal ritrovamento di una singola banconota identificata da una specifica combinazione alfanumerica.
 * 
 * numIterations = Imposta il numero massimo di iterazioni.
 * regParam = Imposta il parametro di regolarizzazione.
 * elasticNetParam = Imposta il parametro di miscelazione ElasticNet.
 * standardization = Se standardizzare le caratteristiche del dataset prima di addestrare il modello.
 * 
 */

/*
SELECT
vanno_rinvenimento,
extract(month from vdata_ind_banconote) as mese_rinvenimento,
vtaglio,
vprovincia,
SUM(vnumero_pezzi) AS numero_pezzi
FROM
sbi_bi.v_segnalazioni_banconote
where vanno_rinvenimento > 1999 and vprima_durante_circol='DURANTE'
GROUP BY
vanno_rinvenimento, extract(month from vdata_ind_banconote),vtaglio,vprovincia
order by 1, 2, 3 LIMIT 300;
 */
object BanconoteFalsificateReg extends SparkJob("SBI-2 - Banconote Falsificate - Analisi di Regressione"){
  
    def run() : Unit = {
			log.info(s"------------------------------------------------------------------------------")
			log.info(s"---- $applicationName start... ----")
			log.info(s"------------------------------------------------------------------------------")
			
			val query = env.getProperty("SELECT_BANCONOTE_FALSIFICATE")
			val storedAs = env.getProperty("sbi.hadoop.format")
			val mode = env.getProperty("sbi.hadoop.mode")
			val numIterations = env.getProperty("sbi.linearegression.numIterations").toInt
			val regParam = env.getProperty("sbi.linearegression.regParam").toDouble
			val elasticNetParam = env.getProperty("sbi.linearegression.elasticNetParam").toDouble
			val standardization = env.getProperty("sbi.linearegression.standardization").toBoolean
			val modelPath = env.getProperty("sbi.linearegression.lrm_model")
			val pipelinePath = env.getProperty("sbi.linearegression.pipeline_model")
			val d_regression_results = env.getProperty("sbi.hadoop.d_regression_results")
			val d_regression_metrics = env.getProperty("sbi.hadoop.d_regression_metrics")
			
			
			log.info(s"SELECT_BANCONOTE_FALSIFICATE =  $query")
			log.info(s"storedAs = $storedAs")
			log.info(s"mode = $mode")
			log.info(s"numIterations = $numIterations")
			log.info(s"regParam = $regParam")
			log.info(s"elasticNetParam = $elasticNetParam")
			log.info(s"standardization = $standardization")
			log.info(s"d_regression_results = $d_regression_results")
			log.info(s"d_regression_metrics = $d_regression_metrics")

			
			val df = spark.sql(query)
			
			
//			SELECT_BANCONOTE_FALSIFICATE
//			vanno_rinvenimento	mese_rinvenimento	vtaglio	vprovincia	numero_pezzi
//			int                	int              	string	string    	int
      val vtaglio_indexer = new StringIndexer().setInputCol("vtaglio").setOutputCol("vtaglio_index").setHandleInvalid("keep")
      val vprovincia_indexer = new StringIndexer().setInputCol("vprovincia").setOutputCol("vprovincia_index").setHandleInvalid("keep")
      
      val inputCols = Array("vtaglio_index","vprovincia_index")
      val outputCols = Array("vtaglio_vec", "vprovincia_vec")
      val encoder = new OneHotEncoderEstimator().setInputCols(inputCols).setOutputCols(outputCols)
      
      val assembler = new VectorAssembler().setInputCols(Array("vanno_rinvenimento", "mese_rinvenimento", "vtaglio_vec", "vprovincia_vec")).setOutputCol("features")
      val stages = Array(vtaglio_indexer, vprovincia_indexer, encoder, assembler)
      
      val pipeline = new Pipeline().setStages(stages)
      val pipelineModel = pipeline.fit(df)
      log.info("fit pipeline")
      
      val pipelineDataDF = pipelineModel.transform(df)
      log.info("transform pipeline")
      
      
      pipelineModel.write.overwrite().save(pipelinePath)
      log.info("pipeline_model.model saved")
      
      
      pipelineDataDF.show(10, false)
      
      
      
      val lr = new LinearRegression()
			  .setFeaturesCol("features")
        .setLabelCol("numero_pezzi")
        .setStandardization(standardization)
        .setMaxIter(numIterations)
        .setRegParam(regParam)
        .setElasticNetParam(elasticNetParam)

        
      // Split the data into training and test sets (30% held out for testing)
      val Array(trainingData, testData) = pipelineDataDF.randomSplit(Array(0.7, 0.3), seed = 1234L)
      

      // Fit the model
      val lrModel = lr.fit(trainingData)
      log.info("fit lrmModel")
      
      
      lrModel.write.overwrite().save(modelPath)
      log.info("lrm_model.model saved")
      
      
      val lrModelEval = lrModel.evaluate(testData)
      log.info("evaluate testData")
      
      val r2Test = lrModelEval.r2
      val rmseTest = lrModelEval.rootMeanSquaredError
      
      // Summarize the model over the training set and print out some metrics
      val trainingSummary = lrModel.summary
      
      
      val residualsDF = trainingSummary.residuals
      
      log.info("residualsDF")
      residualsDF.show()
      
      val predictionsDF = trainingSummary.predictions.toDF()
      
      log.info("predictionsDF")
      predictionsDF.show()
      
      
      val trainingDataDfSchema = predictionsDF.schema
      
			val predResidual = predictionsDF.rdd.map{row =>
			    val numeroPezzi = row.getAs[Double]("numero_pezzi")//(4)
			    val prediction = row.getAs[Double]("prediction")//(10)
			    val residuals = Array[Double](numeroPezzi - prediction)
			    val ret = Row.fromSeq(row.toSeq ++ residuals)
			    ret
      }
      log.info("create Prediction + Residual")
			
			val trainingDataResidualsDfSchema = trainingDataDfSchema.add(StructField("residuals", DoubleType, true))
			
			val retDF = spark.createDataFrame(predResidual, trainingDataResidualsDfSchema)
			
			
			val round = udf((value:Double) => ( BigDecimal( Try(value).getOrElse(0D) ).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble ))
			
			val resultsDF = retDF.select(
			    col("vanno_rinvenimento").cast(IntegerType).alias("vanno_rinvenimento"),
			    col("mese_rinvenimento").cast(IntegerType).alias("mese_rinvenimento"),
			    col("vtaglio"),
			    col("vprovincia"),
			    col("numero_pezzi").cast(LongType).alias("numero_pezzi"),
			    round(col("prediction")).alias("prediction"),
			    round(col("residuals")).alias("residuals"))
			    
      log.info("create results dataframe")
			
      
      retDF.show(20, false)
      
			println(resultsDF.schema)
			
			
			/**
       * save d_regression_results
       */
      resultsDF.coalesce(1).write.format(storedAs).mode(mode).save(d_regression_results)
    	log.info("save d_regression_results OK")
    	
      
      // Print the coefficients and intercept for linear regression
//    	log.info(s"predictionsDF count: ${predictionsDF.count()}")
//      log.info(s"residuals count: ${residualsDF.count()}")
      log.info(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
      log.info(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
      log.info(s"numIterations: ${trainingSummary.totalIterations}")
      log.info(s"RMSE: ${Try(trainingSummary.rootMeanSquaredError).getOrElse(1D)}")
      log.info(s"R2: ${Try(trainingSummary.r2).getOrElse(0D)}")
      log.info(s"explainedVariance: ${Try(lrModelEval.explainedVariance).getOrElse(0D)}")
      log.info(s"RMSE Test: ${Try(lrModelEval.rootMeanSquaredError).getOrElse(1D)}")
      log.info(s"R2 Test: ${Try(lrModelEval.r2).getOrElse(0D)}")
      log.info(s"explainedVariance: ${Try(lrModelEval.explainedVariance).getOrElse(0D)}")
      
      
      val metricsList = Seq(
            Row("Regolarizzazione", regParam),
            Row("ElasticNet", elasticNetParam),
//            Row("Predictions Counts", predictionsDF.count().doubleValue()),
//            Row("Residuals Counts", residualsDF.count().doubleValue()),
            Row("Num Iterations", trainingSummary.totalIterations.doubleValue()),
            Row("RMSE", Try(trainingSummary.rootMeanSquaredError).getOrElse(1D)),
            Row("R2", Try(trainingSummary.r2).getOrElse(0D)),
            Row("Explained Variance", Try(lrModelEval.explainedVariance).getOrElse(0D)),
            Row("RMSE Test", Try(lrModelEval.rootMeanSquaredError).getOrElse(1D)),
            Row("R2 Test", Try(lrModelEval.r2).getOrElse(0D)),
            Row("Explained Variance Test", Try(lrModelEval.explainedVariance).getOrElse(0D))
          )
      
      val metricsRDD = spark.sparkContext.makeRDD(metricsList)
      log.info("create metrics rdd")
      
      val metricsDF = spark.createDataFrame(metricsRDD, Schemas.metricsDF)
      log.info("create metrics dataframe")
          
      metricsDF.show()
      
      
      /**
       * save d_regression_metrics
       */
      metricsDF.write.format(storedAs).mode(mode).save(d_regression_metrics)
    	log.info("save d_regression_metrics OK")
    	
      
      
      val lrModelLoad = LinearRegressionModel.load(modelPath)
      log.info("lrm_model.model loaded")
      
      val predictions = lrModelLoad.transform(testData)
      log.info("lrm_model.model predictions")
      
      log.info("predictions testData")
      predictions.show(10, false)
      
			
			log.info(s"------------------------------------------------------------------------------")
    	log.info(s"----    $applicationName OK    ----")
    	log.info(s"------------------------------------------------------------------------------")
  	}
}