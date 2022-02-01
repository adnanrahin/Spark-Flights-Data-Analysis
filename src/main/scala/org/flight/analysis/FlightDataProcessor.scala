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

}
