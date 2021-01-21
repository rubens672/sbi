package it.pr.sim.ml.carte

import it.pr.sim.ml.SparkJob
import it.pr.sim.ml.schema.Schemas
import org.apache.spark.ml.fpm.FPGrowth
import org.apache.spark.sql.Row
import java.text.SimpleDateFormat
import java.sql.Timestamp

/**
 * REF001.4  Transazioni non riconosciute
 * 
 * Il seguente requisito ha come obiettivo l'identificazione di regole di associazione per la scoperta di analogie relative 
 * alle modalità con cui le frodi si verificano nell'ambito delle Transazioni non riconosciute.
 * 
 * Una buona spiegazione al concetto di regole di associazione e' reperibile su questo sito: 
 * https://towardsdatascience.com/association-rules-2-aa9a77241654
 */

object TransazioniNonRiconosciuteFP extends SparkJob("SBI-2 - Transazioni Non Riconosciute FP"){
  
    def run() : Unit = {
			log.info(s"-------------------------------------------------------------")
			log.info(s"----   $applicationName...   ----")
			log.info(s"-------------------------------------------------------------")
			
			val query = env.getProperty("SELECT_TRANS_NON_RIC_GRAPH_FP")
			val minSupport = env.getProperty("sbi.fpgrowth.minSupport").toDouble
			val minConfidence = env.getProperty("sbi.fpgrowth.minConfidence").toDouble
			val numPartitions = env.getProperty("sbi.fpgrowth.numPartitions").toInt
			val storedAs = env.getProperty("sbi.hadoop.format")
			val mode = env.getProperty("sbi.hadoop.mode")
			val d_trans_non_ric_freq_items = env.getProperty("sbi.hadoop.d_trans_non_ric_freq_items")
			val d_trans_non_ric_association_rules = env.getProperty("sbi.hadoop.d_trans_non_ric_association_rules")
			
			log.info(s"SELECT_TRANS_NON_RIC_GRAPH_FP: $query")
			log.info(s"minSupport = ${minSupport}")
			log.info(s"minConfidence = ${minConfidence}")
			log.info(s"numPartitions = ${numPartitions}")
			log.info(s"storedAs = ${storedAs}")
			log.info(s"mode = ${mode}")
			log.info(s"d_trans_non_ric_freq_items = ${d_trans_non_ric_freq_items}")
			log.info(s"d_trans_non_ric_association_rules = ${d_trans_non_ric_association_rules}")
			
			
			val dataset = spark.sql(query) 
			
			
			val round:String => String = ({ f => 
			  try{
          val ora = f.trim().substring(0,2).toInt
          "00".concat( ora.toString) takeRight 2
			  }catch{
			    case e: Exception =>{"-"}
			  }
      })
      
      /**
       * giorno della settimana
       */
      val dayText = new SimpleDateFormat("E")
			
      def dayOfWeek(data:java.sql.Timestamp): String = {
          dayText.format(data).toUpperCase()
      }
			
			
			import spark.implicits._
      val output = dataset.map(df => {
        try{
            val ret = Seq( ("TIPO_CANALE=" + df.getString(0).trim), 
                            "NOME_CANALE=" + df.getString(1).trim, 
                            "ISSUER=" + df.getString(2).trim , 
                            "GIORNO=" + dayOfWeek(df.getTimestamp(3))) 
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
                                           
      /**
       *  Probability of having milk on the cart with the knowledge that toothbrush is present (i.e. confidence) : 10/(10+4) = 0.7
       * 
			 *	Now to put this number in perspective, consider the probability of having milk on the cart without any knowledge about 
			 *	toothbrush: 80/100 = 0.8
			 *	These numbers show that having toothbrush on the cart actually reduces the probability of having milk on the cart to 0.7 
			 *	from 0.8! This will be a lift of 0.7/0.8 = 0.87. Now that's more like the real picture. A value of lift less than 1 shows 
			 *	that having toothbrush on the cart does not increase the chances of occurrence of milk on the cart in spite of the rule 
			 *	showing a high confidence value. A value of lift greater than 1 vouches for high association between {Y} and {X}. 
			 *	
			 *	More the value of lift, greater are the chances of preference to buy {Y} if the customer has already bought {X}. 
			 *	Lift is the measure that will help store managers to decide product placements on aisle.                                     
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
    	log.info("freqItemsets OK")
    	
    	def get(f:String): (String,String) = {
          val ar = f.split("=")
          (ar(0),ar(1))
      }
			
			val df = freqItemsets.rdd.map(row => {
        
        val itemsArrIn:Seq[String] = row.getSeq(0)
        val freq = row.getAs[Long]("freq")
        
        val col = itemsArrIn.map(f => get(f.toString())).toMap
        
        val tipoCanale = col.getOrElse("TIPO_CANALE", "")
        val nomeCanale = col.getOrElse("NOME_CANALE", "")
        val issuer = col.getOrElse("ISSUER", "")
        val giorno = col.getOrElse("GIORNO", "")
        val items = "[" + itemsArrIn.mkString(",") + "]"
        val itemsArr = itemsArrIn.toArray
        
        Row(tipoCanale, nomeCanale, issuer, giorno, items, itemsArr, freq) 
        
      }).setName("Creazione RDD freqItemsets")
      log.info("Creazione RDD freqItemsets OK")
			
			
      /**
       * creazione dataframe
       */
      val freqItemsetsDF = spark.createDataFrame(df, Schemas.trans_non_ric_freq_items)
      log.info("Creazione DataFrame freqItemsetsDF OK")
      
      freqItemsetsDF.show()
      
      
      /**
       * save d_manomissioni_freq_items
       */
      freqItemsetsDF.write.format(storedAs).mode(mode).save(d_trans_non_ric_freq_items)
    	log.info("save d_trans_non_ric_freq_items OK")
    	
    	
    	/**
    	 * Ottiene le regole di associazione risultanti dall'elaborazione precendente usando minConfidence
    	 */
      val associationRules = model.associationRules
      log.info("associationRules OK")
			
      
      /**
       * crea rdd regole di associazione
       */
      val rounding:Double => Double = ( BigDecimal( _ ).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble )
      
      val dfAssRul = associationRules.rdd.map(row => {
        
        val antArrIn:Seq[String] = row.getSeq(0)
        val consArrIn:Seq[String] = row.getSeq(1)
        val confidence = rounding(row.getAs[Double]("confidence"))
        val lift = rounding(row.getAs[Double]("lift"))
        
        val colAnt = antArrIn.map(f => get(f.toString())).toMap
        val tipoCanaleAnt = colAnt.getOrElse("TIPO_CANALE", "")
        val nomeCanaleAnt = colAnt.getOrElse("NOME_CANALE", "")
        val issuerAnt = colAnt.getOrElse("ISSUER", "")
        val giornoAnt = colAnt.getOrElse("GIORNO", "")
        val itemsAnt = "[" + antArrIn.mkString(",") + "]"
        val itemsArrAnt = antArrIn.toArray
        
        val colCons = consArrIn.map(f => get(f.toString())).toMap
        val tipoCanaleCons = colCons.getOrElse("TIPO_CANALE", "")
        val nomeCanaleCons = colCons.getOrElse("NOME_CANALE", "")
        val issuerCons = colCons.getOrElse("ISSUER", "")
        val giornoCons = colCons.getOrElse("GIORNO", "")
        val itemsCons = "[" + consArrIn.mkString(",") + "]"
        val itemsArrCons = consArrIn.toArray
        
        Row(tipoCanaleAnt, nomeCanaleAnt, issuerAnt, giornoAnt, itemsAnt, itemsArrAnt,
            tipoCanaleCons,nomeCanaleCons,issuerCons, giornoCons,itemsCons,itemsArrCons,
            confidence,lift)
      }).setName("Creazione RDD associationRules")
      
      log.info("Creazione RDD associationRules OK")
      
      
      /**
       * crea dataframe associationRulesDF
       */
      val associationRulesDF = spark.createDataFrame(dfAssRul, Schemas.trans_non_ric_association_rules)
      log.info("Creazione DataFrame associationRulesDF OK")
      
      associationRulesDF.show()
      
      
      /**
       * save d_manomissioni_association_rules
       */
      associationRulesDF.write.format(storedAs).mode(mode).save(d_trans_non_ric_association_rules)
    	log.info("save d_trans_non_ric_association_rules OK")
      
			
			log.info(s"-------------------------------------------------------------")
    	log.info(s"----   $applicationName OK   ----")
    	log.info(s"-------------------------------------------------------------")
  	}
}