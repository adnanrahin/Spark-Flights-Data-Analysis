package org.flight.analysis.dataloader

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.flight.analysis.entity.Flight

class FlightDataLoader(filePath: String, spark: SparkSession) extends DataLoader {

  override def loadRDD(): RDD[Flight] = {
    val flightsCSV: RDD[String] = this.spark.sparkContext.textFile(this.filePath)

    val flightsRDD: RDD[Flight] =
      flightsCSV
        .map(row => row.split(",", -1))
        .map(str => Flight(str(0),
          str(1), str(2), str(3), str(4), str(5), str(6),
          str(7), str(8), str(9), str(10), str(1), str(12),
          str(13), str(14), str(15), str(16), str(17),
          str(18), str(19), str(20), str(21), str(22), str(23),
          str(24), str(25), str(26), str(27), str(28), str(29), str(30))).mapPartitionsWithIndex {
        (idx, iter) => if (idx == 0) iter.drop(1) else iter
      }

    flightsRDD
  }
}
