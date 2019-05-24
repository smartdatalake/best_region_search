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
import org.apache.spark.rdd.RDD

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;
//import java.util.List;
import java.util.Properties;

import scala.util.control.Breaks._
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

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
import matt.definitions.Generic

object OnestepAlgo {

  def oneStepAlgo(input: (Int, Iterable[POI]), eps: Double, decayConstant: Double, topk: Int, finalAnswers: List[POI]): (Int, List[SpatialObjectIdx]) = {

    val pois: java.util.List[POI] = ListBuffer(input._2.toList: _*)
    val scoreFunction = new ScoreFunctionCount[POI]();
    val utilityScoreFunction = new UtilityScoreFunction();

    // Find the blocks.
    val bcaFinder = new BCAIndexProgressiveDivExhaustive(decayConstant, utilityScoreFunction);
    val bca = bcaFinder.findBestCatchmentAreas(pois, eps, topk, scoreFunction).toList;

    // build index
    val index = buildIndex(bca)

    (input._1, index);
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
        if (i != j && Generic.intersects(sp1, sp2) && sp2.compareTo(sp1) < 0) {
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

  def Run(nodeToPoint: RDD[(Int, POI)], eps: Double, decayConstant: Double, topk: Int) {

    //
    var Ans = List[POI]();

    val currentK = topk;
    nodeToPoint.collect().foreach(println)
    val resultGroupedPerNode = nodeToPoint.groupByKey().map(x => oneStepAlgo(x, eps, decayConstant, topk, Ans));
    println(resultGroupedPerNode.collect())
    // combine the graphs into one.
  }
}