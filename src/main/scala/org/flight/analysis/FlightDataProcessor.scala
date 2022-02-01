package org.flight.analysis

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.flight.analysis.dataloader.{AirlineDataLoader, AirportDataLoader, FlightDataLoader}
import org.flight.analysis.entity.{Airline, Airport, Flight}

object FlightDataProcessor {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {

    /**
     * input parameters
     *  1. datasource: path
     * */

    val spark = SparkSession
      .builder()
      .appName("FlightDelaysAndCancellations")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    val dataSourcePath = args(0)

    val flightDataLoader: FlightDataLoader = new FlightDataLoader(dataSourcePath + "flights.csv", spark)
    val flightsRDD: RDD[Flight] = flightDataLoader.loadRDD()

    val airlineDataLoader: AirlineDataLoader = new AirlineDataLoader(dataSourcePath + "airlines.csv", spark)
    val airlineRDD: RDD[Airline] = airlineDataLoader.loadRDD()

    val airportDataLoader: AirportDataLoader = new AirportDataLoader(dataSourcePath + "airports.csv", spark)
    val airportRDD: RDD[Airport] = airportDataLoader.loadRDD()

    showCancelledFlightInDataFrame(flightsRDD, spark)
    airlinesCancelledNumberOfFlightsToDF(flightsRDD, spark, airlineRDD)
    findTotalNumberOfDepartureFlightFromAirportToDF(flightsRDD, airportRDD, "LGA", spark)
    findMostCancelledAirlineToDF(flightsRDD, airlineRDD, spark)
    findAverageDepartureDelayOfAirlinerToDF(flightsRDD, airlineRDD, spark)
    findTotalDistanceFlownEachAirlineToDF(flightsRDD, airlineRDD, spark)
    findOriginAndDestinationByMaxDistanceToDF(flightsRDD, airportRDD, spark)

    spark.close()

  }


  def findAllTheFlightsGetCancelled(flightsRDD: RDD[Flight]): RDD[Flight] = {
    val cancelledFlight =
      flightsRDD
        .filter(flight => flight.cancelled.equals("1"))
        .persist(StorageLevel.MEMORY_ONLY_SER)

    cancelledFlight
  }

  def findAirlinesTotalNumberOfFlightsCancelled(cancelledFlight: RDD[Flight], airlineRDD: RDD[Airline]): List[(String, Int)] = {
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
            case Some(value) => (value, iter._2.toList.size)
            case None => ("Flight IATA Code is wrong", iter._2.toList.size)
          }
        }
        .collect()
        .toList

    airlinesCancelledFlights
  }

  def findTotalNumberOfDepartureFlightFromAirport(flightsRDD: RDD[Flight], airportRDD: RDD[Airport], airportIataCode: String):
  (String, Int) = {

    val airportMap = airportRDD
      .map(airport => (airport.iataCode, airport.airport))
      .collect()
      .toMap

    val notCancelledFlight =
      flightsRDD
        .filter(flight => flight.cancelled.equals("0"))

    val totalFlight: (String, Int) = notCancelledFlight
      .groupBy(flight => flight.originAirport)
      .filter(flight => flight._1.equals(airportIataCode))
      .map { flight =>
        airportMap.get(flight._1) match {
          case Some(value) => (value, flight._2.toList.size)
          case None => (flight._1, flight._2.toList.size)
        }
      }
      .collect()
      .toList.head

    totalFlight
  }

  def findMaxFlightCancelledAirline(flightsRDD: RDD[Flight], airlineRDD: RDD[Airline]): (String, Int) = {

    val cancelledFlightRDD: RDD[Flight] = findAllTheFlightsGetCancelled(flightsRDD)

    val airlineRDDMap = airlineRDD.map(f => (f.iataCode, f.airlineName)).collect().toMap

    val maxCancelledAirliner = cancelledFlightRDD
      .groupBy(_.airline)
      .map(flight => (flight._1, flight._2.toList.size))
      .sortBy(-_._2).collect().toList

    airlineRDDMap.get(maxCancelledAirliner.head._1) match {
      case Some(value) => (value, maxCancelledAirliner.head._2)
      case None => (s"No Such IATA Code ${maxCancelledAirliner.head._1}", maxCancelledAirliner.head._2)
    }

  }

  def findAverageDepartureDelayOfAirliner(flightRDD: RDD[Flight], airlineRDD: RDD[Airline]): List[(String, Double)] = {

    def findAllTheSuccessDelayedFlights(flightRDD: RDD[Flight]): RDD[Flight] = flightRDD
      .filter(flight => flight.cancelled.equals("0") && flight.departureDelay.toInt > 0)

    val airlineRDDMap = airlineRDD
      .map(airline => (airline.iataCode, airline.airlineName))
      .collect()
      .toMap

    val successDelayedFlights: RDD[Flight] = findAllTheSuccessDelayedFlights(flightRDD)

    val averageOfAirliner: List[(String, Double)] = successDelayedFlights
      .groupBy(_.airline)
      .map {
        airline =>
          (airline._1, airline._2.toList.foldLeft(0.0)(_ + _.departureDelay.toInt), airline._2.size)
      }.map {
      airline =>
        airlineRDDMap.get(airline._1) match {
          case Some(value) => (value, airline._2 / airline._3)
          case None => (s"No Such IATA Code ${airline._1}", airline._2 / airline._3)
        }
    }.sortBy(_._2)
      .collect()
      .toList

    averageOfAirliner

  }

  def findTotalDistanceFlownEachAirline(flightsRDD: RDD[Flight], airlineRDD: RDD[Airline]): RDD[(String, Long)] = {

    val airlinePairRDD: RDD[(String, String)] = airlineRDD.map(airline => (airline.iataCode, airline.airlineName))

    val totalAirlineDistance: RDD[(String, Long)] = flightsRDD
      .filter(_.cancelled.equals("0"))
      .groupBy(_.airline)
      .map {
        airline =>
          (airline._1, airline._2.foldLeft(0L)(_ + _.distance.toLong))
      }

    val leftOuterJoinRDD: RDD[(String, (Long, Option[String]))] =
      totalAirlineDistance
        .leftOuterJoin(airlinePairRDD)

    /**
     * NB: Output Data looks like in result
     *
     * (NK,(113749628,Some(Spirit Air Lines)))
     * (WN,(924523336,Some(Southwest Airlines Co.)))
     * (HA,(48160938,Some(Hawaiian Airlines Inc.)))
     * (MQ,(118297439,Some(American Eagle Airlines Inc.)))
     * (DL,(744626128,Some(Delta Air Lines Inc.)))
     * (EV,(257284318,Some(Atlantic Southeast Airlines)))
     * (B6,(279708698,Some(JetBlue Airways)))
     * (F9,(87302103,Some(Frontier Airlines Inc.)))
     * (AS,(206001530,Some(Alaska Airlines Inc.)))
     * (AA,(745699296,Some(American Airlines Inc.)))
     * (UA,(647702424,Some(United Air Lines Inc.)))
     * (OO,(288044533,Some(Skywest Airlines Inc.)))
     * (VX,(86275456,Some(Virgin America)))
     * (US,(178393656,Some(US Airways Inc.)))
     * */

    val result = leftOuterJoinRDD
      .map {
        airline =>
          val airlineName = airline._2._2 match {
            case Some(value) => value
            case None => "None"
          }
          (airlineName, airline._2._1)
      }.sortBy(_._2, ascending = false)

    result

  }

  def findOriginAndDestinationByMaxDistance(flightsRDD: RDD[Flight], airportRDD: RDD[Airport]): (String, String, Long) = {

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

    (source, destination, airportNamesAndDistance._3)
  }

  def findOriginAndDestinationByMaxDistanceToDF(flightsRDD: RDD[Flight], airportRDD: RDD[Airport], spark: SparkSession): Unit = {

    val airportNamesAndDistance: (String, String, Long) =
      findOriginAndDestinationByMaxDistance(flightsRDD, airportRDD)

    spark
      .createDataFrame(List(airportNamesAndDistance))
      .toDF("Source Airport", "Destination Airport", "Total Distance")
      .show(5, truncate = false)
  }

  def findTotalDistanceFlownEachAirlineToDF(flightsRDD: RDD[Flight], airlineRDD: RDD[Airline], spark: SparkSession): Unit = {

    val totalAirlineDistance: RDD[(String, Long)] =
      findTotalDistanceFlownEachAirline(flightsRDD, airlineRDD)

    spark
      .createDataFrame(totalAirlineDistance)
      .toDF("Airline Names", "Total Distance")
      .show(truncate = false)
  }

  def showCancelledFlightInDataFrame(flightsRDD: RDD[Flight], spark: SparkSession): Unit = {

    val cancelledFlight: RDD[Flight] = findAllTheFlightsGetCancelled(flightsRDD)

    spark.createDataFrame(rdd = cancelledFlight)
      .select("airline", "tailNumber", "originAirport", "destinationAirport", "cancellationsReason")
      .show(numRows = 5, truncate = false)
  }

  def airlinesCancelledNumberOfFlightsToDF
  (flightsRDD: RDD[Flight], spark: SparkSession, airlineRDD: RDD[Airline]): Unit = {

    val cancelledFlight: RDD[Flight] = findAllTheFlightsGetCancelled(flightsRDD)

    val airlinesCancelledNumberOfFlights = findAirlinesTotalNumberOfFlightsCancelled(cancelledFlight, airlineRDD)

    spark
      .createDataFrame(airlinesCancelledNumberOfFlights)
      .toDF("Airline Names", "Total Number Of Flight's Cancelled").
      show(truncate = false)
  }

  def findTotalNumberOfDepartureFlightFromAirportToDF
  (flightsRDD: RDD[Flight], airportRDD: RDD[Airport], airportIataCode: String, spark: SparkSession): Unit = {

    val numberOfDepartureFlightFromAirport =
      findTotalNumberOfDepartureFlightFromAirport(flightsRDD, airportRDD, airportIataCode)

    spark
      .createDataFrame(List(numberOfDepartureFlightFromAirport))
      .toDF("Airport Name", "Total Number of Flight's")
      .show(truncate = false)
  }

  def findMostCancelledAirlineToDF(flightsRDD: RDD[Flight], airlineRDD: RDD[Airline], spark: SparkSession): Unit = {

    val mostCancelledAirline = findMaxFlightCancelledAirline(flightsRDD, airlineRDD)

    spark
      .createDataFrame(List(mostCancelledAirline))
      .toDF("Airline Name", "Total Number of Flight's")
      .show(false)
  }

  def findAverageDepartureDelayOfAirlinerToDF(flightsRDD: RDD[Flight], airlineRDD: RDD[Airline], spark: SparkSession): Unit = {

    val delayedAverage = findAverageDepartureDelayOfAirliner(flightsRDD, airlineRDD)

    spark
      .createDataFrame(delayedAverage)
      .toDF("Airline Name", "Average Delay")
      .show(false)
  }

}
