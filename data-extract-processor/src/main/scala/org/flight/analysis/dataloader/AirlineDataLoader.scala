package org.flight.analysis.dataloader

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.flight.analysis.entity.Airline

class AirlineDataLoader(filePath: String, spark: SparkSession) extends DataLoader {

  override def loadRDD(): RDD[Airline] = {

    val airlineCSV: RDD[String] = this.spark.sparkContext.textFile(this.filePath)
    val airlineRDD: RDD[Airline] =
      airlineCSV
        .map(row => row.split(",", -1))
        .map(str => Airline(str(0), str(1))).mapPartitionsWithIndex {
        (idx, iter) => if (idx == 0) iter.drop(1) else iter
      }

    airlineRDD

  }

}
