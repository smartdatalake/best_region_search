package SDL.definitions

import org.apache.spark.sql.types._

object TableDefs {
  val customSchema = StructType(Array(
    StructField("lon", FloatType, true),
    StructField("lat", FloatType, true),
    StructField("name1", StringType, true),
    StructField("name2", StringType, true)))

  val customSchema2 = StructType(Array(
    StructField("id", StringType, true),
    StructField("name", StringType, true),
    StructField("lon", DoubleType, true),
    StructField("lat", DoubleType, true),
    StructField("keywords", StringType, true)))

  val customSchema3 = StructType(Array(
    StructField("id", StringType, true),
    StructField("name", StringType, true),
    StructField("lon", DoubleType, true),
    StructField("lat", DoubleType, true),
    StructField("keywords", StringType, true),
    StructField("score", DoubleType, true)))
}

