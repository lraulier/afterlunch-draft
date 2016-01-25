package org.kzone

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by kubu-kzone on 1/24/16.
  */
object joinDelayWithAirportData {

  def main (args: Array[String]): Unit ={
    val conf = new SparkConf().setAppName("afterlunch-draft")
      .setMaster("local[2]")
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
