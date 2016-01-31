package org.kzone

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by kubu-kzone on 1/24/16.
  */
object joinDelayWithAirportData {
  def isHeader(line: String): Boolean = {
    line.contains("Year")
  }

  def parseDelay(line: String) = {
    val pieces = line.split(',')
    val Year = pieces(0)
    val Month = pieces(1)
    val DayofMonth = pieces(2)
    val DayOfWeek = pieces(3)
    val DepTime = pieces(4)
    val CRSDepTime = pieces(5)
    val ArrTime = pieces(6)
    val CRSArrTime = pieces(7)
    val UniqueCarrier = pieces(8)
    val FlightNum = pieces(9)
    val TailNum = pieces(10)
    val ActualElapsedTime = pieces(11)
    val CRSElapsedTime = pieces(12)
    val AirTime = pieces(13)
    val ArrDelay = pieces(14)
    val DepDelay = pieces(15)
    val Origin = pieces(16)
    val Dest = pieces(17)
    val Distance = pieces(18)
    val TaxiIn = pieces(19)
    val TaxiOut = pieces(20)
    val Cancelled = pieces(21)
    val CancellationCode = pieces(22)
    val Diverted = pieces(23)
    val CarrierDelay = pieces(24)
    val WeatherDelay = pieces(25)
    val NASDelay = pieces(26)
    val SecurityDelay = pieces(27)
    val LateAircraftDelay = pieces(28)

    Delay(Year,Month,DayofMonth,DayOfWeek,DepTime,CRSDepTime,ArrTime,CRSArrTime,UniqueCarrier,FlightNum,TailNum,ActualElapsedTime,
      CRSElapsedTime,AirTime,ArrDelay,DepDelay,Origin,Dest,Distance,TaxiIn,TaxiOut,Cancelled,CancellationCode,Diverted,CarrierDelay,WeatherDelay,
      NASDelay,SecurityDelay,LateAircraftDelay)
  }

  def parseAirport(line: String) ={
    val pieces = line.split(",")
    val AirportId = pieces(0)
    val Name = pieces(1)
    val City =pieces(2)
    val Country = pieces(3)
    val IATA_FAA = pieces(4)
    val ICAO = pieces(5)
    val Latitude =  pieces(6)
    val Longitude =  pieces(7)
    val Altitude = pieces(8)
    val Timezone = pieces(9)

    Airport(AirportId,Name,City,Country,IATA_FAA,ICAO,Latitude,Longitude,Altitude,Timezone)
  }

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
  case class Airport(
                      AirportId: String,
                      Name: String,
                      City: String,
                      Country: String,
                      IATA_FAA: String,
                      ICAO: String,
                      Latitude: String,
                      Longitude: String,
                      Altitude: String,
                      Timezone: String
                    )
  case class Result(
                     code: String,
                     libelle: String,
                     country: String
                   )

  def main (args: Array[String]): Unit ={
    val conf = new SparkConf().setAppName("afterlunch-draft")
      .setMaster("local[2]")

    val sparkContext = new SparkContext(conf)
    val delay = sparkContext.textFile("../data/sample.csv")
    val airport = sparkContext.textFile("../data/airport_data.csv")
    val delay_without_header = delay.filter(!isHeader(_))
    //delay_without_header.foreach(println)
    val  airportData: RDD[Airport] = airport.map(line => parseAirport(line))
    //airportData.foreach(println)

    val delayData: RDD[Delay] = delay_without_header.map(line => parseDelay(line))
    val airportKey: RDD[(String, Airport)] = airportData.keyBy(_.IATA_FAA)
    val delayKey: RDD[(String, Delay)] = delayData.keyBy(f => "\""+f.Origin+"\"")

    val result: RDD[(String, (Airport, Delay))] = airportKey.join(delayKey)

    val output: RDD[Result] = result.map {
       //cannot use implicit parameter
       // incorrect usage count of parameters !???
      (f: (String, (Airport, Delay))) => new Result(f._2._1.IATA_FAA, f._2._1.Name, f._2._1.Country)
    }
    output.foreach(println)
  }
}
