/* SimpleApp.scala */
package matt

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.{ Encoder, Encoders }
import org.apache.spark.sql.functions.{ when, lower, min, max }
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

object OnestepAlgo {

  def oneStepAlgo(input: (Int, Iterable[org.apache.spark.sql.Row]), finalAnswers: List[Row]): (Int, Iterable[org.apache.spark.sql.Row]) = {
    input;
  }

  val customSchema = StructType(Array(
    StructField("longtitude", FloatType, true),
    StructField("latitude", FloatType, true),
    StructField("name1", StringType, true),
    StructField("name2", StringType, true)))

  def extractNode(long: Any, lat: Any, nodes: Int, minmaxLong: (Any, Any), minmaxLat: (Any, Any)): Int = {
    5;
  }

  def poiToKeyValue(x: Row, width: Int, minmaxLong: (Any, Any), minmaxLat: (Any, Any)): (Int, Row) = {
    (extractNode(x.get(0), x.get(1), width, minmaxLong, minmaxLat), x)
  }
  
//  def buildGraph(x: Row

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .appName("Simple Application")
      .getOrCreate()

    import spark.implicits;

    val poiInputFile = "/cloud_store/olma/spark/input/myLPGeu.csv";
    val inputData = spark.read.format("csv").option("header", "true").option("delimiter", ",").schema(customSchema).load(poiInputFile);
    //    inputData.show(false);

    // choose number of expected results
    val requiredResults = 5;

    // set number of cores
    val cores = 24
    val width = scala.math.sqrt(cores).toInt;

    // choose width of grid (the number of cores required is N squared)
    val minmaxLongArray = inputData.agg(min("longtitude"), max("longtitude")).rdd.map(r => r(0)).collect()
    val minmaxLong = (minmaxLongArray.head, minmaxLongArray.last);
    println(minmaxLong);
    val minmaxLatArray = inputData.agg(min("latitude"), max("latitude")).rdd.map(r => r(0)).collect()
    val minmaxLat = (minmaxLatArray.head, minmaxLatArray.last);
    println(minmaxLat);

    // find to which node does each point belongs to : (NodeNo,Row)
    val nodeToPoint = inputData.rdd.map(x => poiToKeyValue(x, width, minmaxLong, minmaxLat));
//    val temp = nodeToPoint.collect();

    //
    val collectedLocalAnswers = List[(Int, Row)]();
    val finalAnswers = List[Row]();

    // here is the actual algorithm

    // build dependency graph.
//    val depGraph = nodeToPoint.map(buildGraph);
    
    // this has to be iterated (each node has to calculate the best subset)
    while (finalAnswers.length <= requiredResults) {

      // calculate the local results at each node.
      val resultGroupedPerNode = nodeToPoint.groupByKey().map(x => oneStepAlgo(x, finalAnswers));

      // sort all results together based on the value of each.

    }

    spark.stop()
  }
}
