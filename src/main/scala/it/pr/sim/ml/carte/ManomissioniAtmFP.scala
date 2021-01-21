package it.pr.sim.ml.carte

import it.pr.sim.ml.SparkJob
import it.pr.sim.ml.schema.Schemas
import org.apache.spark.ml.fpm.FPGrowth
import org.apache.spark.sql.Row
import java.text.SimpleDateFormat
import java.sql.Timestamp

/**
 * REF001.1 Manomissioni ATM Frequent Pattern Mining
 * 
 * Nell'ambito delle manomissioni ATM il sistema dovrà tracciare il modo in cui queste si verificano, 
 * ossia i comportamenti comuni di coloro che manomettono gli sportelli automatici. 
 * A tal fine verranno utilizzate metodologie proprie del data mining per estrarre le relazioni nascoste tra i dati.
 * 
 * La metodologia applicata nel corrente caso d�uso sono algoritmi denominati regole di associazione e più precisamente 
 * l'algoritmo denominato FP-growth.
 * 
 * it.pr.sim.ml.areaCarte.ManomissioniATM
 * 
 * 
 * L'algoritmo FP-growth � descritto nell'articolo 'Han et al., Mining frequent patterns without candidate generation', 
 * dove "FP" sta per frequently pattern (schema frequente). Dato un dataset di transazioni, il primo passo della crescita di FP è quello di calcolare 
 * le frequenze delle voci e identificare le voci frequenti. A differenza degli algoritmi simili ad Apriori progettati per lo stesso scopo, 
 * il secondo passo di FP-growth utilizza una struttura ad albero dei suffissi (FP-tree) per codificare le transazioni senza generare 
 * esplicitamente i set di candidati, che di solito sono costosi da generare. Dopo il secondo passo, i frequenti itemet possono essere 
 * estratti dall'albero FP. In spark.mllib, abbiamo implementato una versione parallela di FP-growth chiamata PFP, come descritto 
 * in Li et al., PFP: Parallel FP-growth for query recommendation. PFP distribuisce il lavoro di crescita di FP-tree basato sui suffissi 
 * delle transazioni, e quindi è più scalabile di un'implementazione a macchina singola. Rimandiamo gli utenti ai documenti per maggiori dettagli.
 * 
 */
object ManomissioniAtmFP  extends SparkJob("SBI-2 - Manomissioni ATM"){
  	def run() : Unit = {
			log.info(s"------------------------------------------------------------")
			log.info(s"----   $applicationName start...           ----")
			log.info(s"------------------------------------------------------------") 
			
			val query = env.getProperty("SELECT_MANOMISSIONE_SPORTELLO")
			val minSupport = env.getProperty("sbi.fpgrowth.minSupport").toDouble
			val minConfidence = env.getProperty("sbi.fpgrowth.minConfidence").toDouble
			val numPartitions = env.getProperty("sbi.fpgrowth.numPartitions").toInt
			val storedAs = env.getProperty("sbi.hadoop.format")
			val mode = env.getProperty("sbi.hadoop.mode")
			val d_manomissioni_freq_items = env.getProperty("sbi.hadoop.d_manomissioni_freq_items")
			val d_manomissioni_association_rules = env.getProperty("sbi.hadoop.d_manomissioni_association_rules")
			
			log.info(s"SELECT_MANOMISSIONE_SPORTELLO: $query")
			log.info(s"minSupport = ${minSupport}")
			log.info(s"minConfidence = ${minConfidence}")
			log.info(s"numPartitions = ${numPartitions}")
			log.info(s"storedAs = ${storedAs}")
			log.info(s"mode = ${mode}")
			log.info(s"d_manomissioni_freq_items = ${d_manomissioni_freq_items}")
			log.info(s"d_manomissioni_association_rules = ${d_manomissioni_association_rules}")
			
			
			val dataset = spark.sql(query) 
			
			/**
			 * arrotondamento dell'ora
			 */
//			val round:String => String = ({ f => 
//			  try{
//          val ora = f.trim().substring(0,2).toInt
//          val minuti = f.trim().substring(2).toInt
//          if(minuti < 30){
//              ("00".concat( ora.toString) takeRight 2)
//          }else{
//              val ret = ora + 1
//              if(ret == 24) "00"
//              else ("00".concat(ret.toString) takeRight 2)
//          }
//			  }catch{
//			    case e: Exception =>{"-"}
//			  }
//      })
			
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
          val ret = Seq( ("ABI=" + df.getString(0).trim), 
                          "CAB=" + df.getString(1).trim, 
                          "ORA=" + round(df.getString(2).trim) , 
                          "TIP_CATTURA=" + df.getString(3).trim, 
                          "TIP_MANOMIS=" + df.getString(4).trim, 
                          "PROV=" + df.getString(5).trim,
                          "COMUNE=" + df.getString(6).trim,
                          "GIORNO=" + dayOfWeek(df.getTimestamp(7))) 
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
       *  minConfidence: fiducia minima per generare la Regola dell'Associazione. La fiducia � un'indicazione di quante volte una regola 
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
    	log.info("freqItemsets OK")
    	
    	def get(f:String): (String,String) = {
          val ar = f.split("=")
          (ar(0),ar(1))
      }
      
      val df = freqItemsets.rdd.map(row => {
        
        val itemsArrIn:Seq[String] = row.getSeq(0)
        val freq = row.getAs[Long]("freq")
        
        val col = itemsArrIn.map(f => get(f.toString())).toMap
        
        val abi = col.getOrElse("ABI", "")
        val cab = col.getOrElse("CAB", "")
        val ora = col.getOrElse("ORA", "")
        val giorno = col.getOrElse("GIORNO", "")
        val tCattura = col.getOrElse("TIP_CATTURA", "")
        val tManomis = col.getOrElse("TIP_MANOMIS", "")
        val prov = col.getOrElse("PROV", "")
        val comune = col.getOrElse("COMUNE", "")
        val items = "[" + itemsArrIn.mkString(",") + "]"
        val itemsArr = itemsArrIn.toArray
        
        Row(abi, cab, ora, giorno, tCattura, tManomis, prov, comune, items, itemsArr, freq) 
        
      }).setName("Creazione RDD freqItemsets")
      log.info("Creazione RDD freqItemsets OK")
      
      
      /**
       * creazione dataframe
       */
      val freqItemsetsDF = spark.createDataFrame(df, Schemas.manomissione_freq_items)
      log.info("Creazione DataFrame freqItemsetsDF OK")
      
      freqItemsetsDF.show()
      
      
      /**
       * save d_manomissioni_freq_items
       */
      freqItemsetsDF.write.format(storedAs).mode(mode).save(d_manomissioni_freq_items)
    	log.info("save d_manomissioni_freq_items OK")
      
    	
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
        val abiAnt = colAnt.getOrElse("ABI", "")
        val cabAnt = colAnt.getOrElse("CAB", "")
        val oraAnt = colAnt.getOrElse("ORA", "")
        val giornoAnt = colAnt.getOrElse("GIORNO", "")
        val tCatturaAnt = colAnt.getOrElse("TIP_CATTURA", "")
        val tManomisAnt = colAnt.getOrElse("TIP_MANOMIS", "")
        val provAnt = colAnt.getOrElse("PROV", "")
        val comuneAnt = colAnt.getOrElse("COMUNE", "")
        val itemsAnt = "[" + antArrIn.mkString(",") + "]"
        val itemsArrAnt = antArrIn.toArray
        
        val colCons = consArrIn.map(f => get(f.toString())).toMap
        val abiCons = colCons.getOrElse("ABI", "")
        val cabCons = colCons.getOrElse("CAB", "")
        val oraCons = colCons.getOrElse("ORA", "")
        val giornoCons = colCons.getOrElse("GIORNO", "")
        val tCatturaCons = colCons.getOrElse("TIP_CATTURA", "")
        val tManomisCons = colCons.getOrElse("TIP_MANOMIS", "")
        val provCons = colCons.getOrElse("PROV", "")
        val comuneCons = colCons.getOrElse("COMUNE", "")
        val itemsCons = "[" + consArrIn.mkString(",") + "]"
        val itemsArrCons = consArrIn.toArray
        
        Row(abiAnt, cabAnt, oraAnt, giornoAnt, tCatturaAnt, tManomisAnt, provAnt, comuneAnt, itemsAnt, itemsArrAnt,
            abiCons,cabCons,oraCons, giornoCons,tCatturaCons,tManomisCons,provCons,comuneCons,itemsCons,itemsArrCons,
            confidence,lift)
      }).setName("Creazione RDD associationRules")
      
      log.info("Creazione RDD associationRules OK")
      
      
      /**
       * crea dataframe associationRulesDF
       */
      val associationRulesDF = spark.createDataFrame(dfAssRul, Schemas.manomissione_association_rules)
      log.info("Creazione DataFrame associationRulesDF OK")
      
      associationRulesDF.show()
      
      
      /**
       * save d_manomissioni_association_rules
       */
      associationRulesDF.write.format(storedAs).mode(mode).save(d_manomissioni_association_rules)
    	log.info("save d_manomissioni_association_rules OK")
      
      
      
			
			log.info(s"------------------------------------------------------------------")
    	log.info(s"----               $applicationName OK           ----")
    	log.info(s"------------------------------------------------------------------")
  	}
}