/* SimpleApp.scala */
package matt.distrib

import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import matt.{DependencyGraph, POI, SpatialObject, SpatialObjectIdx}
import matt.ca.BCAIndexProgressiveDiv
import matt.ca.UtilityScoreFunction
import matt.score.ScoreFunctionCount
import matt.definitions.{Generic, GridIndexer}

object OnestepAlgo {

  def oneStepAlgo(input: (Int, Iterable[POI]), eps: Double, decayConstant: Double, topk: Int, finalAnswers: List[POI],gridIndexer: GridIndexer): DependencyGraph = {

    val pois: java.util.List[POI] = ListBuffer(input._2.toList: _*)
    val scoreFunction = new ScoreFunctionCount[POI]();
    val utilityScoreFunction = new UtilityScoreFunction();
    //val constRoundResult=Iterable[SpatialObject]
    // Find the blocks.
    val bcaFinder = new BCAIndexProgressiveDiv(decayConstant, utilityScoreFunction);
    //while (con)
    val dependencyGraph=new DependencyGraph(input._1,gridIndexer)
    while (dependencyGraph.safeRegionCnt<topk){
      dependencyGraph.add(bcaFinder.findBestCatchmentAreas(pois, eps, topk, scoreFunction).toList);

    }
    dependencyGraph
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

  def Run(nodeToPoint: RDD[(Int, POI)], eps: Double, decayConstant: Double, topk: Int,gridIndexer: GridIndexer) {

    //
    var Ans = List[POI]();

    val currentK = topk;
    nodeToPoint.collect().foreach(println)
    val resultGroupedPerNode = nodeToPoint.groupByKey().map(x => oneStepAlgo(x, eps, decayConstant, topk, Ans,gridIndexer));
    println(resultGroupedPerNode.collect())
    // combine the graphs into one.
  }
}