package com.snehik.covidIndia

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window._

object covidInsight {

  //Problem:1 ==> Which State has highest no of covid case

  def highestCovidCase(indiaData:DataFrame):Row={
     val partitionData = indiaData.repartition(2)
     val covidCasesPerState = partitionData.groupBy(col("State/UnionTerritory")).max("confirmed")
     covidCasesPerState.toDF("State","Total").sort(col("Total").desc).first()
  }

  //Problem:2 ==> Find out infected cases between every two days for all states

  def infectedCaseInRange(indiaData:DataFrame):DataFrame= {
    val seldata = indiaData.select(col("Sno"), col("Date"), col("State/UnionTerritory"), col("Confirmed"))
    val modwbdata = seldata.withColumn("Date", to_date(col("Date"), "dd/MM/yy"))
    val window = partitionBy(col("State/UnionTerritory")).orderBy(col("Date"))
    val prevdateCase = lag(col("Confirmed"), 1).over(window)
    val comparisondata = modwbdata.withColumn("Prev_Date_Confirmed", prevdateCase)
    val compdata = comparisondata.na.fill(0, Seq("Prev_Date_Confirmed"))
    compdata.withColumn("CaseDifference", col("Confirmed") - col("Prev_Date_Confirmed"))
  }
}
