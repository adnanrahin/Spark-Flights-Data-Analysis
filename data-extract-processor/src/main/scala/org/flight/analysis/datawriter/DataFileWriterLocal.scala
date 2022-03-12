package org.flight.analysis.datawriter

import org.apache.spark.sql.{DataFrame, SaveMode}

object DataFileWriterLocal {

  final def dataWriter(dataFrame: DataFrame, dataPath: String, directoryName: String): Unit = {

    val destinationDirectory: String = dataPath + "/" + directoryName

    dataFrame
      .write
      .mode(SaveMode.Overwrite)
      .parquet(destinationDirectory)
  }

}
