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

  def Run(nodeToPoint: RDD[(Int, POI)], eps: Double, decayConstant: Double, topk: Int) {

    //
    var Ans = List[POI]();

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
          if (Generic.intersectsList(localAnswers.get(i), roundAnswers)) {
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

  }
}
