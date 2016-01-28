package org.kzone

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by kubu-kzone on 1/24/16.
  */
object joinDelayWithAirportData {

  def main (args: Array[String]): Unit ={
    val conf = new SparkConf().setAppName("afterlunch-draft")
      .setMaster("local[2]")

    val sparkContext = new SparkContext(conf)

    val delay = sparkContext.textFile("../data/sample.csv")
    val airport = sparkContext.textFile("../data/airport_data.csv")
    val delay_without_header = delay.filter(!isHeader(_))
    //delay_without_header.foreach(println)
    //airport.foreach(println)
    val sampleSplit = delay_without_header.map(line => line.split(","))
    val airportSplit = airport.map(line => line.split(","))
    //airportSplit.foreach(println)
    //sampleSplit.foreach(println)
    val sampleKeyed = sampleSplit.keyBy(cells => "\""+cells(17)+"\"")
    val airportKeyed = airportSplit.keyBy(cells => cells(4))
    //airportKeyed.foreach(println)
    //sampleKeyed.foreach(println)
    val joinedValue = sampleKeyed.join(airportKeyed).map(f => f._2._1(4))
    joinedValue.take(3).foreach(println)
    //joinedVal => joinedVal._2._1(1).toInt + joinedVal._2._2(1).toInt <= 1000)

  }
  def isHeader(line: String): Boolean = {
    line.contains("Year")
  }

}
