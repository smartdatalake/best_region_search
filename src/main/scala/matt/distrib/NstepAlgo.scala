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

import scala.util.control.Breaks._

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

object NstepAlgo {

  def localAlgo(input: (Int, Iterable[POI]), eps: Double, decayConstant: Double, topk: Int, finalAnswers: List[POI]): (Int, List[SpatialObject]) = {

    val pois: java.util.List[POI] = ListBuffer(input._2.toList: _*)

    val scoreFunction = new ScoreFunctionCount[POI]();
    val utilityScoreFunction = new UtilityScoreFunction();
    val distinct = true;

    // compute best catchment areas
    System.out.print("\n\n\n\nComputing best catchment areas...\n\n\n");
    //    startTime = System.nanoTime();
    //    if (exhaustive) {
    //    val bcaFinder = new BCAIndexProgressiveDivExhaustive(decayConstant, utilityScoreFunction);
    //    } else if (div) {
    //    val bcaFinder = new BCAIndexProgressiveDiv(decayConstant, utilityScoreFunction);
    //    } else {
    val bcaFinder = new BCAIndexProgressive(distinct);
    //    }
    val bca = bcaFinder.findBestCatchmentAreas(pois, eps, topk, scoreFunction).toList;
    (input._1, bca);
  }

  def extractNode(long: Any, lat: Any, nodes: Int, minmaxLong: (Any, Any), minmaxLat: (Any, Any)): Int = {
    5;
  }

  def rowToPOI(thisRow: Row, geometryFactory: GeometryFactory): POI = {

    val keywords = thisRow.getAs[String]("keywords").split(",").toList;
    //    print("keyword:\t")
    //    for (keyword <- keywords) {
    //      print(keyword + "\t");
    //    }
    //    println(thisRow);

    val newPOI = new POI(thisRow.getAs[String]("id"), thisRow.getAs[String]("name"), thisRow.getAs[Float]("longtitude"), thisRow.getAs[Float]("latitude"), keywords, 0, geometryFactory);

    //    println("newPOI:\t" + newPOI.getId() + "\t" + newPOI.getName());

    newPOI;
  }

  def poiToKeyValue(x: Row, width: Int, minmaxLong: (Any, Any), minmaxLat: (Any, Any), geometryFactory: GeometryFactory): (Int, POI) = {
    val newPOI = rowToPOI(x, geometryFactory: GeometryFactory);
    val newNode = extractNode(x.get(0), x.get(1), width, minmaxLong, minmaxLat);

    //    println("newPOI:\t" + newPOI.toString());

    (newNode, newPOI)
  }

  def intersects(point1: POI, point2: POI): Boolean = {
    point1.getGeometry().intersects(point2.getGeometry())
  }

  def intersectsList(point: POI, list: ListBuffer[POI]): Boolean = {
    for (point2 <- list) {
      if (intersects(point, point2)) {
        true
      }
    }
    false
  }

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
    // choose number of expected results
    val topk = 10
    val decayConstant = 0.5

    val inputData = spark.read.format("csv").option("header", "true").option("delimiter", ";").schema(TableDefs.customSchema2).load(poiInputFile);
    inputData.show(false);

    // set number of cores
    val cores = 24
    val width = scala.math.sqrt(cores).toInt;

    // choose width of grid (the number of cores required is N squared)
    val minmaxLongArray = inputData.agg(min("longtitude"), max("longtitude")).rdd.map(r => r(0)).collect()
    val minmaxLong = (minmaxLongArray.head, minmaxLongArray.last);
    //    println("\n\n");
    //    println("minmaxLONG: " + minmaxLong);
    //    println("\n\n");
    val minmaxLatArray = inputData.agg(min("latitude"), max("latitude")).rdd.map(r => r(0)).collect()
    val minmaxLat = (minmaxLatArray.head, minmaxLatArray.last);
    //    println("\n\n");
    //    println("minmaxLat: " + minmaxLat);
    //    println("\n\n");

    // find to which node does each point belongs to : (NodeNo,Row)
    val geometryFactory = new GeometryFactory(new PrecisionModel(), 4326);
    val nodeToPoint = inputData.rdd.map(x => poiToKeyValue(x, width, minmaxLong, minmaxLat, geometryFactory));
    //    val temp = nodeToPoint.collect();

    /*
    val collectedPoints = nodeToPoint.take(20);
    for (cur <- collectedPoints) {
      println(cur._1 + "\t" + cur._2.toString());
    }
    */

    //
    var Ans = List[POI]();

    // here is the actual algorithm

    // this has to be iterated (each node has to calculate the best subset)
    var iteration = 0;
    val currentK = topk;
    while (Ans.length <= topk) {

      println("Current Iteration: " + iteration);

      // calculate the local results at each node.
      val resultGroupedPerNode = nodeToPoint.groupByKey().map(x => localAlgo(x, eps, decayConstant, Math.min(topk, currentK), Ans));

      // tranform (node->List) to full List
      //      val localAnswers = resultGroupedPerNode.flatMapValues(f);
      val localAnswers = List[POI]();

      // sort all results together based on the value of each.
      var i = 0;
      var roundAnswers = ListBuffer[POI]();
      breakable {
        while (i < Math.min(topk, currentK)) {

          // if localAnswers[i] overlaps with any result in roundAnswers
          if (intersectsList(localAnswers.get(i), roundAnswers)) {
            break;
          } else {
            val temp = localAnswers.get(i);
            roundAnswers += temp;
          }
        }
      }
      Ans = Ans.++(roundAnswers);

      iteration = iteration + 1;
    }

    println("\n\n\n");
    println("Final Result");
    println("\n\n\n");

    for (x <- Ans) {
      println(x);
    }

    spark.stop()
  }
}
