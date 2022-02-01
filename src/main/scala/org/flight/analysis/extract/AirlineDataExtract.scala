package org.flight.analysis.extract

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.flight.analysis.entity.{Airline, Flight}

object AirlineDataExtract {

  private def findTotalDistanceFlownEachAirline(flightsRDD: RDD[Flight], airlineRDD: RDD[Airline]): RDD[(String, Long)] = {

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

  def findTotalDistanceFlownEachAirlineToDF(flightsRDD: RDD[Flight], airlineRDD: RDD[Airline], spark: SparkSession): Unit = {

    val totalAirlineDistance: RDD[(String, Long)] =
      findTotalDistanceFlownEachAirline(flightsRDD, airlineRDD)

    spark
      .createDataFrame(totalAirlineDistance)
      .toDF("Airline Names", "Total Distance")
      .show(truncate = false)
  }

  private def findAverageDepartureDelayOfAirliner(flightRDD: RDD[Flight], airlineRDD: RDD[Airline]): List[(String, Double)] = {

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

  def findAverageDepartureDelayOfAirlinerToDF(flightsRDD: RDD[Flight], airlineRDD: RDD[Airline], spark: SparkSession): Unit = {

    val delayedAverage = findAverageDepartureDelayOfAirliner(flightsRDD, airlineRDD)

    spark
      .createDataFrame(delayedAverage)
      .toDF("Airline Name", "Average Delay")
      .show(false)
  }


}
