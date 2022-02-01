package org.flight.analysis.extract

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.flight.analysis.entity.{Airline, Flight}

object FlightDataExtract {

  private def findAllTheFlightsGetCancelled(flightsRDD: RDD[Flight]): RDD[Flight] = {
    val cancelledFlight =
      flightsRDD
        .filter(flight => flight.cancelled.equals("1"))
        .persist(StorageLevel.MEMORY_ONLY_SER)

    cancelledFlight
  }

  private def findMaxFlightCancelledAirline(flightsRDD: RDD[Flight], airlineRDD: RDD[Airline]): (String, String) = {

    val cancelledFlightRDD: RDD[Flight] = findAllTheFlightsGetCancelled(flightsRDD)

    val airlineRDDMap = airlineRDD.map(f => (f.iataCode, f.airlineName)).collect().toMap

    val maxCancelledAirliner = cancelledFlightRDD
      .groupBy(_.airline)
      .map(flight => (flight._1, flight._2.toList.size))
      .sortBy(-_._2).collect().toList

    airlineRDDMap.get(maxCancelledAirliner.head._1) match {
      case Some(value) => (value, maxCancelledAirliner.head._2.toString)
      case None => (s"No Such IATA Code ${maxCancelledAirliner.head._1}", maxCancelledAirliner.head._2.toString)
    }

  }

  private def findAirlinesTotalNumberOfFlightsCancelled(cancelledFlight: RDD[Flight],
                                                        airlineRDD: RDD[Airline]): List[(String, String)] = {
    val lookupMAP =
      airlineRDD
        .map(f => (f.iataCode, f.airlineName))
        .collect()
        .toMap

    val airlinesCancelledFlights =
      cancelledFlight
        .groupBy(_.airline)
        .map { iter =>
          lookupMAP.get(iter._1) match {
            case Some(value) => (value, iter._2.toList.size.toString)
            case None => ("Flight IATA Code is wrong", iter._2.toList.size.toString)
          }
        }
        .collect()
        .toList

    airlinesCancelledFlights
  }

  def airlinesCancelledNumberOfFlightsToDF
  (flightsRDD: RDD[Flight], spark: SparkSession, airlineRDD: RDD[Airline]): DataFrame = {

    val cancelledFlight: RDD[Flight] = findAllTheFlightsGetCancelled(flightsRDD)

    val airlinesCancelledNumberOfFlights = findAirlinesTotalNumberOfFlightsCancelled(cancelledFlight, airlineRDD)

    spark
      .createDataFrame(airlinesCancelledNumberOfFlights)
      .toDF("Airline_Names", "Total_Number_Of_Flights_Cancelled")
  }

  def showCancelledFlightInDataFrame(flightsRDD: RDD[Flight], spark: SparkSession): DataFrame = {

    val cancelledFlight: RDD[Flight] = findAllTheFlightsGetCancelled(flightsRDD)

    spark.createDataFrame(rdd = cancelledFlight)
      .select("airline", "tailNumber", "originAirport", "destinationAirport", "cancellationsReason")

  }

  def findMostCancelledAirlineToDF(flightsRDD: RDD[Flight], airlineRDD: RDD[Airline], spark: SparkSession): DataFrame = {

    val mostCancelledAirline = findMaxFlightCancelledAirline(flightsRDD, airlineRDD)

    spark
      .createDataFrame(List(mostCancelledAirline))
      .toDF("Airline_Name", "Total_Number_of_Flight's")
      
  }

}
