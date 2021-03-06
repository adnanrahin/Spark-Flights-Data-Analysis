package org.flight.analysis.extract

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.flight.analysis.entity.{Airport, Flight}

object AirportDataExtract {

  private def findOriginAndDestinationByMaxDistance(flightsRDD: RDD[Flight],
                                                    airportRDD: RDD[Airport]): (String, String, String) = {

    val airportNamesAndDistance: (String, String, Long) = flightsRDD
      .filter(flight => flight.diverted.equals("0"))
      .map(flight => (flight.originAirport, flight.destinationAirport, flight.distance.toLong))
      .max()(Ordering[Long].on(x => x._3))

    val airportPairRDD: RDD[(String, String)] =
      airportRDD
        .filter(airport => airport.iataCode.equals(airportNamesAndDistance._1)
          || airport.iataCode.equals(airportNamesAndDistance._2))
        .map(airport => (airport.iataCode, airport.airport))

    val source: String = airportPairRDD.distinct().lookup(airportNamesAndDistance._1)(0)
    val destination: String = airportPairRDD.distinct().lookup(airportNamesAndDistance._2)(0)

    (source, destination, airportNamesAndDistance._3.toString)
  }

  def findOriginAndDestinationByMaxDistanceToDF(flightsRDD: RDD[Flight],
                                                airportRDD: RDD[Airport],
                                                spark: SparkSession): DataFrame = {

    val airportNamesAndDistance =
      findOriginAndDestinationByMaxDistance(flightsRDD, airportRDD)

    spark
      .createDataFrame(List(airportNamesAndDistance))
      .toDF("Source_Airport", "Destination_Airport", "Total_Distance")
  }

  private def findTotalNumberOfDepartureFlightFromAirport(flightsRDD: RDD[Flight],
                                                          airportRDD: RDD[Airport],
                                                          airportIataCode: String):
  (String, String) = {

    val airportMap = airportRDD
      .map(airport => (airport.iataCode, airport.airport))
      .collect()
      .toMap

    val notCancelledFlight =
      flightsRDD
        .filter(flight => flight.cancelled.equals("0"))

    val totalFlight: (String, String) = notCancelledFlight
      .groupBy(flight => flight.originAirport)
      .filter(flight => flight._1.equals(airportIataCode))
      .map { flight =>
        airportMap.get(flight._1) match {
          case Some(value) => (value, flight._2.toList.size.toString)
          case None => (flight._1, flight._2.toList.size.toString)
        }
      }
      .collect()
      .toList.head

    totalFlight
  }

  def findTotalNumberOfDepartureFlightFromAirportToDF(flightsRDD: RDD[Flight],
                                                      airportRDD: RDD[Airport],
                                                      airportIataCode: String,
                                                      spark: SparkSession): DataFrame = {

    val numberOfDepartureFlightFromAirport =
      findTotalNumberOfDepartureFlightFromAirport(flightsRDD, airportRDD, airportIataCode)

    spark
      .createDataFrame(List(numberOfDepartureFlightFromAirport))
      .toDF("Airport_Name", "Total_Number_of_Flights")
  }

}
