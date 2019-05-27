package matt.definitions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.{ Encoder, Encoders }
import org.apache.spark.sql.functions.{ when, lower, min, max }
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

object TableDefs {
  val customSchema = StructType(Array(
    StructField("longtitude", FloatType, true),
    StructField("latitude", FloatType, true),
    StructField("name1", StringType, true),
    StructField("name2", StringType, true)))

  val customSchema2 = StructType(Array(
    StructField("id", StringType, true),
    StructField("name", StringType, true),
    StructField("longtitude", DoubleType, true),
    StructField("latitude", DoubleType, true),
    StructField("keywords", StringType, true)))
}

