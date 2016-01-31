package org.kzone

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql._

/**
  * Created by kubu-kzone on 1/24/16.
  */
object joinDelayWithAirportData {

  def main (args: Array[String]): Unit ={
    val conf = new SparkConf().setAppName("afterlunch-draft")
      .setMaster("local[2]")

    val sparkContext = new SparkContext(conf)

    // sc is an existing SparkContext.
    val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)
    val df: DataFrame = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load("../data/sample.csv")
    df.show() // show content

    // define case class Delay
    case class Delay(Year: String,
                     Month: String,
                     DayofMonth: String,
                     DayOfWeek: String,
                     DepTime: String,
                     CRSDepTime: String,
                     ArrTime: String,
                     CRSArrTime: String,
                     UniqueCarrier: String,
                     FlightNum: String,
                     TailNum: String,
                     ActualElapsedTime: String,
                     CRSElapsedTime: String,
                     AirTime: String,
                     ArrDelay: String,
                     DepDelay: String,
                     Origin: String,
                     Dest: String,
                     Distance: String,
                     TaxiIn: String,
                     TaxiOut: String,
                     Cancelled: String,
                     CancellationCode: String,
                     Diverted: String,
                     CarrierDelay: String,
                     WeatherDelay: String,
                     NASDelay: String,
                     SecurityDelay: String,
                     LateAircraftDelay: String)
  }
}
