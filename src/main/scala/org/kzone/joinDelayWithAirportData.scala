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
    val delay: DataFrame = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load("../data/sample.csv")
    delay.show() // show content
    //delay.printSchema()
    delay.registerTempTable("delay")
    //sqlContext.sql("select Year from delay").foreach(println)

    val airport: DataFrame = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "true")
      .load("../data/airport_data.csv")
    //airport.printSchema()
    airport.show()
    airport.registerTempTable("airport")

    sqlContext.sql("select distinct(C2) from delay,airport where airport.C4=delay.Origin ").foreach(println)
  }
}
