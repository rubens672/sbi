package it.pr.sim.dataPreparation

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.log4j.{Level, Logger}

import it.pr.sim.SparkJob
import it.pr.sim.exception.Exception._

import java.io.File
import scala.util.Try

/**
 * nella creazione - ad esempio - di trans_non_ric, che esiste una gerarchia: 
 * prima bisogna far passare la creazione di transazioni_canale, poi quella di trans_non_ric
 */

object dTransazioniCanale extends SparkJob("Data Preparation SBI - D_TRANSAZIONI_CANALE", "it.pr.sim.dataPreparation.dTransazioniCanale") {
  def run() : Unit = {
			  val truncateQuery = env.getProperty("TRUNCATE_D_TRANSAZIONI_CANALE")
			  val file = env.getProperty("sbi.data.preparation.dTransazioniCanale")
			  val query = sourceHeper.readFromHDFS(repoPath.concat(file)).mkString
			  new ExecuteQuery(applicationName, truncateQuery, query).run(true)  
  }
}

object vSegnalazioniBanconoteUno extends SparkJob("Data Preparation SBI - V_SEGNALAZIONI_BANCONOTE_UNO", "it.pr.sim.dataPreparation.vSegnalazioniBanconoteUno") {
  def run() : Unit = {
			  val truncateQuery = env.getProperty("TRUNCATE_V_SEGNALAZIONI_BANCONOTE")
			  val file = env.getProperty("sbi.data.preparation.vSegnalazioniBanconoteUno")
			  val query = sourceHeper.readFromHDFS(repoPath.concat(file)).mkString
			  new ExecuteQuery(applicationName, truncateQuery, query).run(true)  
  }
}

object vSegnalazioniBanconoteDue extends SparkJob("Data Preparation SBI - V_SEGNALAZIONI_BANCONOTE_DUE", "it.pr.sim.dataPreparation.vSegnalazioniBanconoteDue") {
  def run() : Unit = {
			  val file = env.getProperty("sbi.data.preparation.vSegnalazioniBanconoteDue")
			  val query = sourceHeper.readFromHDFS(repoPath.concat(file)).mkString
			  new ExecuteQuery(applicationName, "", query).run(false)  
  }
}

object vSegnalazioniMoneteUno extends SparkJob("Data Preparation SBI - V_SEGNALAZIONI_MONETE_UNO", "it.pr.sim.dataPreparation.vSegnalazioniMoneteUno") {
  def run() : Unit = {
			  val truncateQuery = env.getProperty("TRUNCATE_V_SEGNALAZIONI_MONETE")
			  val file = env.getProperty("sbi.data.preparation.vSegnalazioniMoneteUno")
			  val query = sourceHeper.readFromHDFS(repoPath.concat(file)).mkString
			  new ExecuteQuery(applicationName, truncateQuery, query).run(true)  
  }
}

object vSegnalazioniMoneteDue extends SparkJob("Data Preparation SBI - V_SEGNALAZIONI_MONETE_DUE", "it.pr.sim.dataPreparation.vSegnalazioniMoneteDue") {
  def run() : Unit = {
			  val file = env.getProperty("sbi.data.preparation.vSegnalazioniMoneteDue")
			  val query = sourceHeper.readFromHDFS(repoPath.concat(file)).mkString
			  new ExecuteQuery(applicationName, "", query).run(false)  
  }
}

object vTransNonRic extends SparkJob("Data Preparation SBI - V_TRANS_NON_RIC", "it.pr.sim.dataPreparation.vTransNonRic") {
  def run() : Unit = {
			  val truncateQuery = env.getProperty("TRUNCATE_V_TRANS_NON_RIC")
			  val file = env.getProperty("sbi.data.preparation.vTransNonRic")
			  val query = sourceHeper.readFromHDFS(repoPath.concat(file)).mkString
			  new ExecuteQuery(applicationName, truncateQuery, query).run(true)  
  }
}

object dTransazioniValuta extends SparkJob("Data Preparation SBI - D_TRANSAZIONI_VALUTA", "it.pr.sim.dataPreparation.dTransazioniValuta") {
  def run() : Unit = {
			  val truncateQuery = env.getProperty("TRUNCATE_D_TRANSAZIONI_VALUTA")
			  val file = env.getProperty("sbi.data.preparation.dTransazioniValuta")
			  val query = sourceHeper.readFromHDFS(repoPath.concat(file)).mkString
			  new ExecuteQuery(applicationName, truncateQuery, query).run(true)  
  }
}