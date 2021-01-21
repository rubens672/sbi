package it.pr.sim.ml.schema

import org.apache.spark.sql.types._

object Schemas {
  
  val manomissione_freq_items = StructType(Array(
      StructField("ABI", StringType, true),
      StructField("CAB", StringType, true),
      StructField("ORA", StringType, true),
      StructField("GIORNO", StringType, true),
      StructField("TIP_CATTURA", StringType, true),
      StructField("TIP_MANOMIS", StringType, true),
      StructField("PROV", StringType, true),
      StructField("COMUNE", StringType, true),
      StructField("ITEMS", StringType, false),
      
      StructField("ITEMS_ARR", ArrayType(StringType,true), false), 
      StructField("FREQ", LongType, false)
      )
  )
  
  
    val manomissione_association_rules = StructType(Array(
      StructField("ABI_ANT", StringType, true),
      StructField("CAB_ANT", StringType, true),
      StructField("ORA_ANT", StringType, true),
      StructField("GIORNO_ANT", StringType, true),
      StructField("TIP_CATTURA_ANT", StringType, true),
      StructField("TIP_MANOMIS_ANT", StringType, true),
      StructField("PROV_ANT", StringType, true),
      StructField("COMUNE_ANT", StringType, true),
      StructField("ITEMS_ANT", StringType, false),
      StructField("ITEMS_ARR_ANT", ArrayType(StringType,true), false),
      
      StructField("ABI_CONS", StringType, true),
      StructField("CAB_CONS", StringType, true),
      StructField("ORA_CONS", StringType, true),
      StructField("GIORNO_CONS", StringType, true),
      StructField("TIP_CATTURA_CONS", StringType, true),
      StructField("TIP_MANOMIS_CONS", StringType, true),
      StructField("PROV_CONS", StringType, true),
      StructField("COMUNE_CONS", StringType, true),
      StructField("ITEMS_CONS", StringType, false),
      StructField("ITEMS_ARR_CONS", ArrayType(StringType,true), false),
      
      StructField("CONFIDENCE", DoubleType, false),
      StructField("LIFT", DoubleType, false)
      )
  )
  
    val trans_non_ric_triplets = StructType(Array(
      StructField("SRC_ID", LongType, true),
      StructField("SRC_LABEL", StringType, true),
      StructField("SRC_DESCRIPTION", StringType, true),
      StructField("SRC_DENOMINAZIONE", StringType, true),
      StructField("DST_ID", LongType, true),
      StructField("DST_LABEL", StringType, true),
      StructField("DST_DESCRIPTION", StringType, true),
      StructField("DST_DENOMINAZIONE", StringType, true),
      StructField("ATTR_LABEL", StringType, true),
      StructField("ATTR_WEIGHT", DoubleType, true),
      StructField("ATTR_DATA_TRANSAZIONE", TimestampType, false)
//      ,StructField("ATTR_ANNO_TRANSAZIONE", IntegerType, false)
      )
  )
  
  val correlazione_multipla = StructType(Array(
            StructField("X", StringType, true),
            StructField("Y", StringType, true),
            StructField("value", DoubleType, true)))
            
    val indicatori_sintesi = StructType(Array(
            StructField("X", StringType, true),
            StructField("Y", StringType, true),
            StructField("value", DoubleType, true),
            StructField("label_canale", StringType, true)))
            
  val trans_non_ric_freq_items = StructType(Array(
      StructField("TIPO_CANALE", StringType, true),
      StructField("NOME_CANALE", StringType, true),
      StructField("ISSUER", StringType, true),
      StructField("GIORNO", StringType, true),
      StructField("ITEMS", StringType, false),
      
      StructField("ITEMS_ARR", ArrayType(StringType,true), false), 
      StructField("FREQ", LongType, false)
      )
  )       
     
  val trans_non_ric_association_rules = StructType(Array(
      StructField("TIPO_CANALE_ANT", StringType, true),
      StructField("NOME_CANALE_ANT", StringType, true),
      StructField("ISSUER_ANT", StringType, true),
      StructField("GIORNO_ANT", StringType, true),
      StructField("ITEMS_ANT", StringType, false),
      StructField("ITEMS_ARR_ANT", ArrayType(StringType,true), false),
      
      StructField("TIPO_CANALE_CONS", StringType, true),
      StructField("NOME_CANALE_CONS", StringType, true),
      StructField("ISSUER_CONS", StringType, true),
      StructField("GIORNO_CONS", StringType, true),
      StructField("ITEMS_CONS", StringType, false),
      StructField("ITEMS_ARR_CONS", ArrayType(StringType,true), false),
      
      StructField("CONFIDENCE", DoubleType, false),
      StructField("LIFT", DoubleType, false)
      )
  )
  
    val banconote_falsificate_freq_items = StructType(Array(
      StructField("ABI", StringType, true),
      StructField("CAB", StringType, true),
      StructField("TAGLIO", StringType, true),
      StructField("NUMERO_PEZZI", StringType, true),
      StructField("PROV", StringType, true),
      StructField("COMUNE", StringType, true),
      StructField("LUOGO", StringType, true),
      StructField("GIORNO", StringType, true),
      StructField("MESE", StringType, true),
      StructField("ITEMS", StringType, false),
      
      StructField("ITEMS_ARR", ArrayType(StringType,true), false), 
      StructField("FREQ", LongType, false)
      )
  )
  
  val banconote_falsificate_freq_items_2 = StructType(Array(
      StructField("TAGLIOSERIE", StringType, true),
      StructField("PROV", StringType, true),
      StructField("LUOGO_RINVENIMENTO", StringType, true),
      StructField("ITEMS", StringType, false),
      StructField("ITEMS_ARR", ArrayType(StringType,true), false),
      StructField("FREQ", LongType, false)
      )
  )
  
  val banconote_falsificate_rules_2 = StructType(Array(
      StructField("TAGLIOSERIE_ANT", StringType, true),
      StructField("PROV_ANT", StringType, true),
      StructField("LUOGO_RINVENIMENTO_ANT", StringType, true),
      StructField("ITEMS_ANT", StringType, false),
      StructField("ITEMS_ARR_ANT", ArrayType(StringType,true), false),
      
      StructField("TAGLIOSERIE_CONS", StringType, true),
      StructField("PROV_CONS", StringType, true),
      StructField("LUOGO_RINVENIMENTO_CONS", StringType, true),
      StructField("ITEMS_CONS", StringType, false),
      StructField("ITEMS_ARR_CONS", ArrayType(StringType,true), false),
      
      StructField("CONFIDENCE", DoubleType, false),
      StructField("LIFT", DoubleType, false)
      )
  )
  
   val banconote_falsificate_rules = StructType(Array(
      StructField("ABI_ANT", StringType, true),
      StructField("CAB_ANT", StringType, true),
      StructField("TAGLIO_ANT", StringType, true),
      StructField("NUMERO_PEZZI_ANT", StringType, true),
      StructField("PROV_ANT", StringType, true),
      StructField("COMUNE_ANT", StringType, true),
      StructField("LUOGO_ANT", StringType, true),
      StructField("GIORNO_ANT", StringType, true),
      StructField("MESE_ANT", StringType, true),
      StructField("ITEMS_ANT", StringType, false),
      StructField("ITEMS_ARR_ANT", ArrayType(StringType,true), false),
      
      StructField("ABI_CONS", StringType, true),
      StructField("CAB_CONS", StringType, true),
      StructField("TAGLIO_CONS", StringType, true),
      StructField("NUMERO_PEZZI_CONS", StringType, true),
      StructField("PROV_CONS", StringType, true),
      StructField("COMUNE_CONS", StringType, true),
      StructField("LUOGO_CONS", StringType, true),
      StructField("GIORNO_CONS", StringType, true),
      StructField("MESE_CONS", StringType, true),
      StructField("ITEMS_CONS", StringType, false),
      StructField("ITEMS_ARR_CONS", ArrayType(StringType,true), false),
      
      StructField("CONFIDENCE", DoubleType, false),
      StructField("LIFT", DoubleType, false)
      )
  )
  
    val resultsDF = StructType(Array(
       StructField("VANNORINVENIMENTO", IntegerType, true),
       StructField("MESE_RINVENIMENTO", IntegerType, true),
       StructField("VTAGLIO", StringType, true),
       StructField("VPROVINCIA", StringType, true),
       StructField("NUMERO_PEZZI", LongType, true),
       StructField("PREDICTION", DoubleType, true),
       StructField("RESIDUALS", DoubleType, true)
      ))
  
  val metricsDF = StructType(Array(
       StructField("PARAM", StringType, true),
       StructField("VALUE", DoubleType, true)
      ))
  
}