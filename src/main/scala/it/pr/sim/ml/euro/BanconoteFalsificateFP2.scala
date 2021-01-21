package it.pr.sim.ml.euro

import it.pr.sim.ml.SparkJob
import it.pr.sim.ml.schema.Schemas
import org.apache.spark.ml.fpm.FPGrowth
import org.apache.spark.sql.Row
import java.text.SimpleDateFormat
import java.sql.Timestamp

/**
SELECT 
vtaglio,
vserie,
vtaglio,
vnumero_pezzi,
vprovincia,
vdesc_provincia,
luogo_rinvenimento
FROM sbi_BI.V_SEGNALAZIONI_BANCONOTE s
--JOIN sbi_BI.d_luogo_rinvenimento l ON l.sequ_luogo_rinvenimento=s.vfk_luogo_rinvenimento
WHERE luogo_rinvenimento <> '-'

LIMIT 10;


it.pr.sim.ml.areaEuro.BanconoteFalsificateFP

 */

object BanconoteFalsificateFP2  extends SparkJob("SBI-2 - Banconote Falsificate FP 2"){
  	def run() : Unit = {
  	  log.info(s"------------------------------------------------------------")
			log.info(s"----   SBI-2 - Banconote Falsificate FP start...   ----")
			log.info(s"------------------------------------------------------------")
  	  
			
			val query = env.getProperty("SELECT_BANCONOTE_FALSIFICATE_FREQUENT_PATTERN_2")
			val minSupport = env.getProperty("sbi.fpgrowth.minSupport").toDouble
			val minConfidence = env.getProperty("sbi.fpgrowth.minConfidence").toDouble
			val numPartitions = env.getProperty("sbi.fpgrowth.numPartitions").toInt
			val storedAs = env.getProperty("sbi.hadoop.format")
			val mode = env.getProperty("sbi.hadoop.mode")
  		val d_banconote_falsificate_freq_items_2 = env.getProperty("sbi.hadoop.d_banconote_falsificate_freq_items_2")
			val d_banconote_falsificate_association_rules_2 = env.getProperty("sbi.hadoop.d_banconote_falsificate_association_rules_2")
			
			log.info(s"query: $query")
			log.info(s"minSupport = ${minSupport}")
			log.info(s"minConfidence = ${minConfidence}")
			log.info(s"numPartitions = ${numPartitions}")
			log.info(s"storedAs = ${storedAs}")
			log.info(s"mode = ${mode}")
//			log.info(s"d_manomissioni_freq_items = ${d_banconote_falsificate_freq_items}")
			log.info(s"d_banconote_falsificate_association_rules_2 = ${d_banconote_falsificate_association_rules_2}")
			
			
			val dataset = spark.sql(query) 
			
			
			/*/** ora del giorno */
			val round:String => String = ({ f => 
			  try{
          val ora = f.trim().substring(0,2).toInt
          "00".concat( ora.toString) takeRight 2
			  }catch{
			    case e: Exception =>{"-"}
			  }
      })
      
      /** giorno della settimana */
      val dayText = new SimpleDateFormat("E")
      def dayOfWeek(data:Timestamp): String = {
          dayText.format(data).toUpperCase()
      }
			
			/** giorno della settimana */
      val monthText = new SimpleDateFormat("MMMM")
      def monthInYear(data:Timestamp): String = {
          monthText.format(data).toUpperCase()
      }*/
			
      
      
      import spark.implicits._
      val output = dataset.map(df => {
        
        try{
              val ret = Seq(
                            "TAGLIOSERIE=" + df.getString(0).trim ,  
                            "PROV=" + df.getString(1).trim,
                            "LUOGO=" + df.getString(2).trim 
                                  ) 
              ret
          }catch{
        	  case e: Exception =>{null}
        	}
        }           
      ).filter(row => row != null).toDF("items")
      
      
      /**
       *	minSupport: il supporto minimo per un set di campioni da identificare come frequenti. 
       * 							Ad esempio, se un elemento appare in 3 transazioni su 5, ha un supporto di 3/5=0,6. 
       * 
       *  minConfidence: fiducia minima per generare la Regola dell'Associazione. La fiducia è un'indicazione di quante volte una regola 
       *  							 di associazione è stata trovata vera. Ad esempio, se nelle transazioni le voci del gruppo X appaiono 4 volte, X e Y 
       *  							 coincidono solo 2 volte, la confidenza per la regola X => Y è quindi 2/4 = 0,5. Il parametro non influisce sul mining per 
       *  							 gli itemset frequenti, ma specifica la confidenza minima per generare regole di associazione per gli itemset frequenti.
       *  
       *  numPartitions:  il numero di partizioni utilizzate per distribuire il lavoro sul cluster. Per impostazione predefinita il parametro non è 
       *  								impostato e viene utilizzato il numero di partizioni del dataset di input.
       */ 
      val fpgrowth = new FPGrowth().setItemsCol("items")
                             .setMinSupport(minSupport)
                             .setMinConfidence(minConfidence)
                             .setNumPartitions(numPartitions) 
                             
                             
                             
      val model = fpgrowth.fit(output)
      
      /**
      * Visualizza i set di voci frequenti
      */
      val freqItemsets = model.freqItemsets
                             
      def get(f:String): (String,String) = {
          val ar = f.split("=")
          (ar(0),ar(1))
      }   
      
      
      import org.apache.spark.sql.types._

      val df = freqItemsets.rdd.map(row => {
              val itemsArrIn:Seq[String] = row.getSeq(0)
              val freq = row.getAs[Long]("freq")
              
              val col = itemsArrIn.map(f => get(f.toString())).toMap
              
              val taglioserie = col.getOrElse("TAGLIOSERIE", "")
              val provincia = col.getOrElse("PROV", "")
              val luogo = col.getOrElse("LUOGO_RINVENIMENTO", "")
              val items = "[" + itemsArrIn.mkString(",") + "]"
              val itemsArr = itemsArrIn.toArray
              
              Row(taglioserie, provincia,  luogo,  items, itemsArr, freq) 
      }).setName("Creazione RDD freqItemsets")
      
      /**
       * creazione dataframe
       */
    
      val freqItemsetsDF = spark.createDataFrame(df, Schemas.banconote_falsificate_freq_items_2)
      log.info("Creazione DataFrame freqItemsetsDF OK")
      freqItemsetsDF.show()
      
      /**
       * save d_manomissioni_freq_items
       */
      freqItemsetsDF.write.format(storedAs).mode(mode).save(d_banconote_falsificate_freq_items_2)
    	log.info("save d_banconote_falsificate_freq_items_2 OK")      
      
      
      
      val associationRules = model.associationRules
      
      val rounding:Double => Double = ( BigDecimal( _ ).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble )      
      
      
      val dfAssRul = associationRules.rdd.map(row => {
        
        val antArrIn:Seq[String] = row.getSeq(0)
        val consArrIn:Seq[String] = row.getSeq(1)
        val confidence = rounding(row.getAs[Double]("confidence"))
        val lift = rounding(row.getAs[Double]("lift"))
        
        val colAnt = antArrIn.map(f => get(f.toString())).toMap
        val taglioserieAnt = colAnt.getOrElse("TAGLIOSERIE", "")
        val provAnt = colAnt.getOrElse("PROV", "")
        val luogoAnt = colAnt.getOrElse("LUOGO_RINVENIMENTO", "")
        val itemsAnt = "[" + antArrIn.mkString(",") + "]"
        val itemsArrAnt = antArrIn.toArray
        
        val colCons = consArrIn.map(f => get(f.toString())).toMap
        val taglioserieCons = colCons.getOrElse("TAGLIOSERIE", "")
        val provCons = colCons.getOrElse("PROV", "")
        val luogoCons = colCons.getOrElse("LUOGO_RINVENIMENTO", "")
        val itemsCons = "[" + consArrIn.mkString(",") + "]"
        val itemsArrCons = consArrIn.toArray
        
        Row(taglioserieAnt,provAnt,luogoAnt,itemsAnt, itemsArrAnt,
            taglioserieCons,provCons,luogoCons,itemsCons,itemsArrCons,
            confidence,lift)
      }).setName("Creazione RDD associationRules")
      
      
      /**
       * crea dataframe associationRulesDF
       */
      val associationRulesDF = spark.createDataFrame(dfAssRul, Schemas.banconote_falsificate_rules_2)
      log.info("Creazione DataFrame associationRulesDF OK")
      
      associationRulesDF.show()
      /**
       * save d_manomissioni_association_rules
       */
      associationRulesDF.write.format(storedAs).mode(mode).save(d_banconote_falsificate_association_rules_2)
    	log.info("save d_banconote_falsificate_association_rules OK")
      
      
			log.info(s"------------------------------------------------------------")
    	log.info(s"----     SBI-2 - Banconote Falsificate FP 2 OK       ----")
    	log.info(s"------------------------------------------------------------")
  	}
  
}