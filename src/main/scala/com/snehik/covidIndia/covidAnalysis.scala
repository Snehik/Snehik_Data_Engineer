package com.snehik.covidIndia


import java.util.Properties

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import com.snehik.covidIndia.covidDataread._
import com.snehik.covidIndia.covidInsight._
import scala.io.Source

object covidAnalysis extends Serializable {

  def getSparkAppConf: SparkConf = {
    val sparkAppConf = new SparkConf
    //Set all Spark Configs
    val props = new Properties
    props.load(Source.fromFile("spark.conf").bufferedReader())
    props.forEach((k, v) => sparkAppConf.set(k.toString, v.toString))
    sparkAppConf
  }

  @transient lazy val logger:Logger = Logger.getLogger(getClass.getName)

  def main(args:Array[String])={
    if (args.length == 0) {
      logger.error("Usage: HelloSpark filename")
      System.exit(1)
    }
    logger.info("Starting Covid Application")

    val spark = SparkSession.builder()
      .config(getSparkAppConf)
      .getOrCreate()

    val indiaData = loadIndiaData(spark,args(0))
    val hospitalBedData = loadHospitalBedData(spark,args(1))
    val ageGroupData = loadAgeGroupData(spark,args(2))
    val icmrData = loadICMRData(spark,args(3))
    val individualData = loadIndividualData(spark,args(4))
    val indiaPopulation = loadPopulationIndiaData(spark,args(5))
    val stateTesting = loadStatewiseData(spark,args(6))

    val StateCount = highestCovidCase(indiaData)
    val caseDetails = infectedCaseInRange(indiaData)
    logger.info("India Highest Covid Infected State: "+ StateCount.mkString(" --> "))
    
    logger.info("Finished Covid Application")

    spark.stop()
  }
}
