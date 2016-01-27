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
    airport.foreach(println)
    val sampleSplit = delay_without_header.map(line => line.split(","))
    //sampleSplit.foreach(println)
  }
  def isHeader(line: String): Boolean = {
    line.contains("Year")
  }
  def parseDelay(line: String) = {
    val pieces = line.split(',')
    val Year = pieces(0).toInt
    val Month = pieces(1).toInt
    val DayofMonth = pieces(2).toInt
    val DayOfWeek = pieces(3).toInt
    val DepTime = pieces(4).toInt
    val CRSDepTime = pieces(5).toInt
    val ArrTime = pieces(6).toInt
    val CRSArrTime = pieces(7).toInt
    val UniqueCarrier = pieces(8)
    val FlightNum = pieces(9).toInt
    val TailNum = pieces(10)
    val ActualElapsedTime = pieces(11).toInt
    val CRSElapsedTime = pieces(12).toInt
    val AirTime = pieces(13).toInt
    val ArrDelay = pieces(14).toInt
    val DepDelay = pieces(15).toInt
    val Origin = pieces(16)
    val Dest = pieces(17)
    val Distance = pieces(18).toInt
    val TaxiIn = pieces(19).toInt
    val TaxiOut = pieces(20).toInt
    val Cancelled = pieces(21).toInt
    val CancellationCode = pieces(22).toInt
    val Diverted = pieces(23).toInt
    val CarrierDelay = pieces(24).toInt
    val WeatherDelay = pieces(25).toInt
    val NASDelay = pieces(26).toInt
    val SecurityDelay = pieces(27).toInt
    val LateAircraftDelay = pieces(28).toInt

    Delay(Year,Month,DayofMonth,DayOfWeek,DepTime,CRSDepTime,ArrTime,CRSArrTime,UniqueCarrier,FlightNum,TailNum,ActualElapsedTime,
      CRSElapsedTime,AirTime,ArrDelay,DepDelay,Origin,Dest,Distance,TaxiIn,TaxiOut,Cancelled,CancellationCode,Diverted,CarrierDelay,WeatherDelay,
      NASDelay,SecurityDelay,LateAircraftDelay)
  }
 case class Delay(Year: Int,
            Month: Int,
            DayofMonth: Int,
            DayOfWeek: Int,
            DepTime: Int,
            CRSDepTime: Int,
            ArrTime: Int,
            CRSArrTime: Int,
            UniqueCarrier: String,
            FlightNum: Int,
            TailNum: String,
            ActualElapsedTime: Int,
            CRSElapsedTime: Int,
            AirTime: Int,
            ArrDelay: Int,
            DepDelay: Int,
            Origin: String,
            Dest: String,
            Distance: Int,
            TaxiIn: Int,
            TaxiOut: Int,
            Cancelled: Int,
            CancellationCode: Int,
            Diverted: Int,
            CarrierDelay: Int,
            WeatherDelay: Int,
            NASDelay: Int,
            SecurityDelay: Int,
            LateAircraftDelay: Int)
  
}
