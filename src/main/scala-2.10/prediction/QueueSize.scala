package prediction

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

/**
  * Created by srastogi on 26-Nov-16.
  */
object QueueSize {

  val IS_HEADER_PRESENT = "true"
  val PARSING_MODE = "PERMISSIVE"
  val DELIMITER = ","
  val DATE_FORMAT = "dd-MMM-yyyy HH:mm:ss"

  val flightSchema: StructType = new StructType(Array(
    StructField("flt_carrier_code", StringType, false),
    StructField("flt_number", StringType, false),
    StructField("seg_departure_datetime", TimestampType, false),
    StructField("seg_board_point", StringType, false),
    StructField("seg_off_point", StringType, false)
  ))

  def readFlightSchedules(sqlContext: SQLContext, dumpPath: String) = {
    sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", IS_HEADER_PRESENT)
      .option("mode", PARSING_MODE)
      .option("delimiter", DELIMITER)
      .option("dateFormat", DATE_FORMAT)
      .schema(flightSchema)
      .load(dumpPath)
  }

}
