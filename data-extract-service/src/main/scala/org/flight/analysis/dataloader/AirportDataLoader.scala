package org.flight.analysis.dataloader

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.flight.analysis.entity.Airport

class AirportDataLoader(filePath: String, spark: SparkSession) extends DataLoader {

  override def loadRDD(): RDD[Airport] = {


    val airportCSV: RDD[String] = this.spark.sparkContext.textFile(this.filePath)

    val airportRDD: RDD[Airport] =
      airportCSV
        .map(row => row.split(",", -1))
        .map(str => Airport(str(0), str(1), str(2), str(3), str(4), str(5), str(6)))
        .mapPartitionsWithIndex {
          (idx, iter) => if (idx == 0) iter.drop(1) else iter
        }

    airportRDD

  }

}
