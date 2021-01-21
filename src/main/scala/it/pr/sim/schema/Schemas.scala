package it.pr.sim.schema

import org.apache.spark.sql.types._

object Schemas {
  
  val schemaTabelleDaGeolocalizzare = StructType(Array(
          StructField("sequ_tabella", LongType, true),
          StructField("nome_tabella_geolocalizzata", StringType, true),
          StructField("desc_nome_regione", StringType, true),
          StructField("desc_sigla_provincia", StringType, true),
          StructField("desc_comune", StringType, true),
          StructField("indi_indirizzo", StringType, true)
      ))
  
  val schemaGeolocalizzazione = StructType(Array(
          StructField("SEQU_GEOLOCALIZZAZIONE", StringType, true), 
          StructField("NOME_TABELLA_GEOLOCALIZZATA",StringType, true), 
          StructField("FK_TABELLA_GEOLOCALIZZATA", LongType, true),
          StructField("INDIRIZZO_INPUT", StringType, true),
          StructField("COMUNE_INPUT", StringType, true),
          StructField("SIGLA_PROVINCIA_INPUT", StringType, true),
          StructField("REGIONE_INPUT", StringType, true),
          StructField("INDIRIZZO_NORM", StringType, true),
          StructField("COMUNE_NORM", StringType, true),
          StructField("SIGLA_PROVINCIA_NORM", StringType, true),
          StructField("CAP_NORM", StringType, true),
          StructField("LATITUDINE", DoubleType, true),
          StructField("LONGITUDINE", DoubleType, true),
          StructField("FLAG_SUCCESSO_SI_NO", StringType, true),
          StructField("ROW_CREATED_USER", StringType, true),
          StructField("STATUS_LINE", StringType, true),
          StructField("ROW_CREATED_DTTM", TimestampType, true)))
          
  val schemaValoreFinanziario = StructType(Array(
            StructField("data_osservazione", TimestampType, true),
            StructField("totale_circolante", LongType, true)))
            
  val schemaMerchantCategGroup = StructType(Array(
            StructField("codice_mcc", StringType, true),
            StructField("descrizione_mcc", StringType, true),
            StructField("codice_mcg", StringType, true),
            StructField("descrizione_mcg", StringType, true)))
            
  val schemaDPopolazione = StructType(Array(
            StructField("itter107", StringType, true),
            StructField("territorio", StringType, true),
            StructField("tipo_dato15", StringType, true),
            StructField("tipo_di_indicatore_demografico", StringType, true),
            StructField("sexist1", StringType, true),
            StructField("sesso", StringType, true),
            StructField("eta1", StringType, true),
            StructField("eta", StringType, true),
            StructField("statciv2", StringType, true),
            StructField("stato_civile", StringType, true),
            StructField("time", IntegerType, true),
            StructField("seleziona_periodo", StringType, true),
            StructField("value", LongType, true)))
            
  val schemaSuccursaliBanche = StructType(Array(
            StructField("id_int", LongType, true),
            StructField("den_160", StringType, true),
            StructField("cod_mecc", StringType, true),
            StructField("id_sede", StringType, true),
            StructField("cab_sede", StringType, true),
            StructField("caus_aper", StringType, true),
            StructField("des_caus_aper", StringType, true),
            StructField("data_i_oper", TimestampType, true),
            StructField("data_f_oper", TimestampType, true),
            StructField("caus_chius", StringType, true),
            StructField("des_caus_chius", StringType, true),
            StructField("indirizzo", StringType, true),
            StructField("cap", StringType, true),
            StructField("frazione", StringType, true),
            StructField("cab_com_ita", StringType, true),
            StructField("des_com_ita", StringType, true),
            StructField("cod_loc_estera", StringType, true),
            StructField("des_loc_estera", StringType, true),
            StructField("stato", LongType, true),
            StructField("des_stato", StringType, true),
            StructField("tipo_sede", StringType, true),
            StructField("des_tipo_sede", StringType, true),
            StructField("data_i_val", TimestampType, true),
            StructField("data_f_val", TimestampType, true)))
            
  val schemaTotaliTransazioniGlobale = StructType(Array(
            StructField("anno_riferimento", IntegerType, true),
            StructField("tipo_dato", StringType, true),
            StructField("numero_transazioni", LongType, true),
            StructField("valore_transazioni", LongType, true)))
            
  val schemaValoriAtmPos = StructType(Array(
            StructField("cubeid", StringType, true),
            StructField("durata_originaria_della_applicazione", StringType, true),
            StructField("residenza", StringType, true),
            StructField("divisa", StringType, true),
            StructField("fenomeno_economico", StringType, true),
            StructField("localizzazione_dello_sportello", StringType, true),
            StructField("data_dell_osservazione", TimestampType, true),
            StructField("ente_segnalante", StringType, true),
            StructField("valore", LongType, true)))
}