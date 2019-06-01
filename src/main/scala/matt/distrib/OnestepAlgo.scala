/* SimpleApp.scala */
package matt.distrib

import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import matt.{DependencyGraph, POI, SpatialObject, SpatialObjectIdx}
import matt.ca.{BCAIndexProgressiveDiv, BCAIndexProgressiveOneRound, UtilityScoreFunction}
import matt.score.ScoreFunctionCount
import matt.definitions.{Generic, GridIndexer}
import matt.distrib.NstepAlgo.localAlgo

import scala.util.control.Breaks.break

object OnestepAlgo {

  def oneStepAlgo(input: (Int, Iterable[POI]), eps: Double, decayConstant: Double, topk: Int, gridIndexer: GridIndexer): List[SpatialObject] = {

    val pois: java.util.List[POI] = ListBuffer(input._2.toList: _*)
    val scoreFunction = new ScoreFunctionCount[POI]();
    val utilityScoreFunction = new UtilityScoreFunction();
    val distinct = true
    //val constRoundResult=Iterable[SpatialObject]
    // Find the blocks.
    val bcaFinder = new BCAIndexProgressiveOneRound(distinct, gridIndexer);
    //while (con)

    bcaFinder.findBestCatchmentAreas(pois, eps, topk, scoreFunction).asInstanceOf[List[SpatialObject]]

  }

  def buildIndex(bca: List[SpatialObject]): List[SpatialObjectIdx] = {

    var finalIndex = ListBuffer[SpatialObjectIdx]();

    var i = 0;
    for (sp1 <- bca) {

      // create new index entry
      //var newSpatialObjectIndex = new matt.SpatialObjectIdx(i);

      // find all overlapping rectangles with this rectangle.
      var j = 0;
      for (sp2 <- bca) {
        if (i != j && Generic.intersects(sp1, sp2) && sp2.compareTo(sp1) < 0) {
          // if the other rectangle has a lower score then insert it to our dependency list.
          //   newSpatialObjectIndex.addDependency(j);
        }
        j = j + 1;
      }

      //   finalIndex += newSpatialObjectIndex;

      i = i + 1;
    }

    finalIndex.toList
  }

  //  def buildGraph(x: Row

  def Run(nodeToPoint: RDD[(Int, POI)], eps: Double, decayConstant: Double, topk: Int, gridIndexer: GridIndexer) {

    //
    var Ans = ListBuffer[SpatialObject]();
    val currentK = topk;
    //nodeToPoint.collect().foreach(println)
    val resultGroupedPerNode = nodeToPoint.groupByKey().flatMap(x => oneStepAlgo(x, eps, decayConstant, topk, gridIndexer));
    val localAnswers = resultGroupedPerNode.collect().toList.sortBy(_.getScore).reverse
    println("***********************************************************************************************************************************")
    println(localAnswers.size)

    var pos = 0

    while (Ans.size < topk && pos != localAnswers.size) {
      if (!Generic.intersectsList(localAnswers.get(pos), Ans))
        Ans.add(localAnswers.get(pos))
      pos += 1
    }
    println("\n\n\n");
    println("Final Result");
    println("\n\n\n");

    for (x <- Ans) {
      println(x.getId);
    }
  }
}