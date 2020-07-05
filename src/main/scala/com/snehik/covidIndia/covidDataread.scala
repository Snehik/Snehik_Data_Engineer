package com.snehik.covidIndia

import org.apache.spark.sql.{DataFrame, SparkSession}

object covidDataread {
  def loadIndiaData(spark:SparkSession,dataFile:String): DataFrame ={
    spark.read
      .option("header","true")
      .option("inferSchema","true")
      .csv(dataFile)
  }
  def loadHospitalBedData(spark:SparkSession,dataFile:String):DataFrame={
    spark.read
      .option("header","true")
      .option("inferSchema","true")
      .csv(dataFile)
  }
  def loadAgeGroupData(spark:SparkSession,dataFile:String):DataFrame={
    spark.read
      .option("header","true")
      .option("inferSchema","true")
      .csv(dataFile)
  }
  def loadICMRData(spark:SparkSession,dataFile:String):DataFrame={
    spark.read
      .option("header","true")
      .option("inferSchema","true")
      .csv(dataFile)
  }
  def loadIndividualData(spark:SparkSession,dataFile:String):DataFrame={
    spark.read
      .option("header","true")
      .option("inferSchema","true")
      .csv(dataFile)
  }
  def loadPopulationIndiaData(spark:SparkSession,dataFile:String):DataFrame={
    spark.read
      .option("header","true")
      .option("inferSchema","true")
      .csv(dataFile)
  }
  def loadStatewiseData(spark:SparkSession,dataFile:String):DataFrame={
    spark.read
      .option("header","true")
      .option("inferSchema","true")
      .csv(dataFile)
  }
}
