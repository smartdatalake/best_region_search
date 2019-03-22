/* SimpleApp.scala */
package matt.distrib

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.{ Encoder, Encoders }
import org.apache.spark.sql.functions.{ when, lower, min, max }
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;
//import java.util.List;
import java.util.Properties;

import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;

import matt.POI;
import matt.SpatialObject;
import matt.Grid;
import matt.ca.BCAFinder;
import matt.ca.BCAIndexProgressive;
import matt.ca.BCAIndexProgressiveDiv;
import matt.ca.BCAIndexProgressiveDivExhaustive;
import matt.ca.UtilityScoreFunction;
import matt.io.InputFileParser;
import matt.io.ResultsWriter;
import matt.score.ScoreFunction;
import matt.score.ScoreFunctionCount;

import matt.definitions.TableDefs

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

object OnestepAlgo {

  def oneStepAlgo(input: (Int, Iterable[POI]), eps: Double, decayConstant: Double, topk: Int, finalAnswers: List[POI]): (Int, List[SpatialObject]) = {
    input;
  }

  def extractNode(long: Any, lat: Any, nodes: Int, minmaxLong: (Any, Any), minmaxLat: (Any, Any)): Int = {
    5;
  }

  def rowToPOI(thisRow: Row, geometryFactory: GeometryFactory): POI = {

    val keywords = thisRow.getAs[String]("keywords").split(",").toList;

    val newPOI = new POI(thisRow.getAs("id"), thisRow.getAs("name"), thisRow.getAs("longtitude"), thisRow.getAs("latitude"), keywords, 0, geometryFactory);

    println(newPOI);

    newPOI
  }

  def poiToKeyValue(x: Row, width: Int, minmaxLong: (Any, Any), minmaxLat: (Any, Any), geometryFactory: GeometryFactory): (Int, POI) = {
    (extractNode(x.get(0), x.get(1), width, minmaxLong, minmaxLat), rowToPOI(x, geometryFactory: GeometryFactory))
  }

  //  def buildGraph(x: Row

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .appName("Simple Application")
      .getOrCreate()

    import spark.implicits;

    //		/* load configuration file */
    //		val prop = new Properties();
    //		prop.load(new FileInputStream("config.properties"));
    //
    //		val eps = prop.getProperty("ca-eps").toDouble;
    //		val topk = prop.getProperty("ca-topk").toInt;
    //		val distinct = prop.getProperty("ca-distinct").toBoolean;
    //		val div = prop.getProperty("ca-div").toBoolean;
    //		val exhaustive = prop.getProperty("ca-exhaustive").toBoolean
    //		val decayConstant = prop.getProperty("ca-decay-constant").toDouble;
    //		val printResults = true;

    val poiInputFile = "/cloud_store/olma/spark/input/osmpois-europe.csv";

    val eps = 0.001
    val topk = 10
    val decayConstant = 0.5

    val inputData = spark.read.format("csv").option("header", "true").option("delimiter", ";").schema(TableDefs.customSchema2).load(poiInputFile);
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
    val geometryFactory = new GeometryFactory(new PrecisionModel(), 4326);
    val nodeToPoint = inputData.rdd.map(x => poiToKeyValue(x, width, minmaxLong, minmaxLat, geometryFactory));
    //    val temp = nodeToPoint.collect();

    //
    val collectedLocalAnswers = List[(Int, Row)]();
    val finalAnswers = List[POI]();

    // build index

    // calculate the local results at each node.
    val resultGroupedPerNode = nodeToPoint.groupByKey().map(x => oneStepAlgo(x, finalAnswers));

    // sort all results together based on the value of each.

    spark.stop()
  }
}
