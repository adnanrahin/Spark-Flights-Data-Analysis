package org.flight.analysis

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.flight.analysis.dataloader.{AirlineDataLoader, AirportDataLoader, FlightDataLoader}
import org.flight.analysis.datawriter.DataFileWriterLocal
import org.flight.analysis.entity.{Airline, Airport, Flight}
import org.flight.analysis.extract.AirlineDataExtract.{findAverageDepartureDelayOfAirlinerToDF, findTotalDistanceFlownEachAirlineToDF}
import org.flight.analysis.extract.AirportDataExtract.{findOriginAndDestinationByMaxDistanceToDF, findTotalNumberOfDepartureFlightFromAirportToDF}
import org.flight.analysis.extract.FlightDataExtract.{airlinesCancelledNumberOfFlightsToDF, findMostCancelledAirlineToDF}

import java.lang.reflect.InvocationTargetException

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
      .master("spark://ubuntu:7077")
      .getOrCreate()

    val sc = spark.sparkContext

    val dataSourcePath = args(0)

    val flightDataLoader: FlightDataLoader = new FlightDataLoader(dataSourcePath + "flights.csv", spark)
    val flightsRDD: RDD[Flight] = flightDataLoader.loadRDD()

    val airlineDataLoader: AirlineDataLoader = new AirlineDataLoader(dataSourcePath + "airlines.csv", spark)
    val airlineRDD: RDD[Airline] = airlineDataLoader.loadRDD()

    val airportDataLoader: AirportDataLoader = new AirportDataLoader(dataSourcePath + "airports.csv", spark)
    val airportRDD: RDD[Airport] = airportDataLoader.loadRDD()

    val dataPath = args(1)

    args(1) match {

      case "1" => {
        val airlinesCancelledNumberOfFlights: DataFrame = {
          airlinesCancelledNumberOfFlightsToDF(flightsRDD, spark, airlineRDD)
        }
        DataFileWriterLocal.dataWriter(dataFrame = airlinesCancelledNumberOfFlights,
          dataPath = dataPath,
          directoryName = "number_of_cancelled_flights")
      }

      case "2" => {
        val findTotalNumberOfDepartureFlightFromAirport: DataFrame = {
          findTotalNumberOfDepartureFlightFromAirportToDF(flightsRDD, airportRDD, "LGA", spark)
        }
        DataFileWriterLocal.dataWriter(dataFrame = findTotalNumberOfDepartureFlightFromAirport,
          dataPath = dataPath,
          directoryName = "find_total_number_departure_flight")
      }

      case "3" => {
        val findMostCancelledAirline: DataFrame = {
          findMostCancelledAirlineToDF(flightsRDD, airlineRDD, spark)
        }
        DataFileWriterLocal.dataWriter(dataFrame = findMostCancelledAirline,
          dataPath = dataPath,
          directoryName = "find_most_cancelled_airline")
      }

      case "4" => {
        val findAverageDepartureDelayOfAirliner: DataFrame = {
          findAverageDepartureDelayOfAirlinerToDF(flightsRDD, airlineRDD, spark)
        }
        DataFileWriterLocal.dataWriter(dataFrame = findAverageDepartureDelayOfAirliner,
          dataPath = dataPath,
          directoryName = "find_average_departure_delay")
      }

      case "5" => {
        val findTotalDistanceFlownEachAirline: DataFrame = {
          findTotalDistanceFlownEachAirlineToDF(flightsRDD, airlineRDD, spark)
        }
        DataFileWriterLocal.dataWriter(dataFrame = findTotalDistanceFlownEachAirline,
          dataPath = dataPath,
          directoryName = "find_total_distance_flown")
      }

      case "6" => {
        val findOriginAndDestinationByMaxDistance: DataFrame = {
          findOriginAndDestinationByMaxDistanceToDF(flightsRDD, airportRDD, spark)
        }
        DataFileWriterLocal.dataWriter(dataFrame = findOriginAndDestinationByMaxDistance,
          dataPath = dataPath,
          directoryName = "find_origin_and_dest_by_max_distance")
      }

      case _ => {
        try {

        } catch {
          case exception: ArrayIndexOutOfBoundsException => println("Array index out of bound, args(1) is missing from programs argument " + exception)
          case exception: InvocationTargetException => println("Missing parameters from run arguments " + exception)
        } finally {
          spark.close()
        }
      }
    }

    spark.close()

  }

}
