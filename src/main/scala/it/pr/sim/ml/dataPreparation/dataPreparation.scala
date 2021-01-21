package it.pr.sim.ml.dataPreparation

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.log4j.{Level, Logger}

import it.pr.sim.ml.exception.Exception._

import java.io.File
import scala.util.Try

import it.pr.sim.ml.SparkJob


object vDenominazioneEnte extends SparkJob("SBI-2 - Data Preparation - V_DENOMINAZIONE_ENTE") {
  def run() : Unit = {
			  val file = env.getProperty("sbi.data.preparation.vDenominazioneEnte")
			  val query = sourceHeper.readFromHDFS(repoPath.concat(file)).mkString
			  new ExecuteQuery(applicationName, query).run(true)  
  }
}

object vManomissioneSportello extends SparkJob("SBI-2 - Data Preparation - V_MANOMISSIONE_SPORTELLO") {
  def run() : Unit = {
			  val file = env.getProperty("sbi.data.preparation.vManomissioneSportello")
			  val query = sourceHeper.readFromHDFS(repoPath.concat(file)).mkString
			  new ExecuteQuery(applicationName, query).run(true)  
  }
}

object vTransNonRicGraph extends SparkJob("SBI-2 - Data Preparation - V_TRANS_NON_RIC_GRAPH") {
  def run() : Unit = {
			  val file = env.getProperty("sbi.data.preparation.vTransNonRicGraph")
			  val query = sourceHeper.readFromHDFS(repoPath.concat(file)).mkString
			  new ExecuteQuery(applicationName, query).run(true)  
  }
}