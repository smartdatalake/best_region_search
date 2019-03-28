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
import matt.SpatialObjectIdx;
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

  def oneStepAlgo(input: (Int, Iterable[POI]), eps: Double, decayConstant: Double, topk: Int, finalAnswers: List[POI]): (Int, List[SpatialObjectIdx]) = {

    val pois: java.util.List[POI] = ListBuffer(input._2.toList: _*)

    val scoreFunction = new ScoreFunctionCount[POI]();

    val utilityScoreFunction = new UtilityScoreFunction();

    val bcaFinder = new BCAIndexProgressiveDivExhaustive(decayConstant, utilityScoreFunction);

    val bca = bcaFinder.findBestCatchmentAreas(pois, eps, topk, scoreFunction).toList;

    val index = buildIndex(bca)

    (input._1, index);
  }

  def overlap(obj1: SpatialObject, obj2: SpatialObject): Boolean = {
    true;
  }

  def buildIndex(bca: List[SpatialObject]): List[SpatialObjectIdx] = {

    var finalIndex = ListBuffer[SpatialObjectIdx]();

    var i = 0;
    for (sp1 <- bca) {

      // create new index entry
      var newSpatialObjectIndex = new matt.SpatialObjectIdx(i);

      // find all overlapping rectangles with this rectangle.
      var j = 0;
      for (sp2 <- bca) {
        if (i != j && overlap(sp1, sp2) && sp2.compareTo(sp1) < 0) {
          // if the other rectangle has a lower score then insert it to our dependency list.
          newSpatialObjectIndex.addDependency(j);
        }
        j = j + 1;
      }

      finalIndex += newSpatialObjectIndex;

      i = i + 1;
    }

    finalIndex.toList
  }

  //  def buildGraph(x: Row

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .appName("Simple Application")
      .getOrCreate()

    import spark.implicits;

    val poiInputFile = "/cloud_store/olma/spark/input/osmpois-europe.csv";

    val eps = 0.001
    val topk = 50
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
    val resultGroupedPerNode = nodeToPoint.groupByKey().map(x => oneStepAlgo(x, eps, decayConstant, topk, finalAnswers));

    // sort all results together based on the value of each.

    spark.stop()
  }
}