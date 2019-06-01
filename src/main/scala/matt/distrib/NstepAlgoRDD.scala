/* SimpleApp.scala */
package matt.distrib


import matt.Grid
import matt.POI
import matt.SpatialObject
import matt.score.ScoreFunction
import org.locationtech.jts.geom.Envelope
import org.locationtech.jts.geom.GeometryFactory
import org.locationtech.jts.geom.PrecisionModel
import java.util
import java.util.PriorityQueue

import matt.{Grid, POI, SpatialObject}
import matt.ca.{BCAIndexProgressive, BCAIndexProgressive2, BCAIndexProgressiveRDD, Block}
import matt.definitions.Generic
import matt.score.ScoreFunctionCount
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

object NstepAlgo {

  def localAlgo(input: (Int, Iterable[POI]), eps: Double, topk: Int, finalAnswers: List[SpatialObject]): List[SpatialObject] = {

    val pois: java.util.List[POI] = ListBuffer(input._2.toList: _*)

    val scoreFunction = new ScoreFunctionCount[POI]();
    val distinct = true;

    // compute best catchment areas
    System.out.print("\n\n\n\nComputing best catchment areas...\n\n\n");
    val bcaFinder = new BCAIndexProgressive(distinct);
    //    }
   return bcaFinder.findBestCatchmentAreas(pois, eps, topk, scoreFunction, finalAnswers).toList;
  }

  def Run(nodeToPoint: RDD[(Int, POI)], eps: Double,  K: Int,width:Int) {

    //
    import matt.ca.BCAIndexProgressive
    val bcaFinder = new BCAIndexProgressiveRDD(true)
    var Ans = List[SpatialObject]();
    val indexToGrid:mutable.HashMap[Int,Grid]=new mutable.HashMap[Int,Grid]()
    val indexToBCA:mutable.HashMap[Int,BCAIndexProgressive2]=new mutable.HashMap[Int,BCAIndexProgressive2]()
    val indexToQueue:mutable.HashMap[Int,PriorityQueue[Block]]=new mutable.HashMap[Int,PriorityQueue[Block]]()
    nodeToPoint.groupByKey().foreach(x=>{indexToBCA.add((x._1,new BCAIndexProgressive2(x._2.toList, eps, new ScoreFunctionCount[POI]())))})
    // this has to be iterated (each node has to calculate the best subset)
    var iteration = 0;
    val Kprime =5;
    while (Ans.length < K) {

      println("Current Iteration: " + iteration);
      var localAnswers = List[SpatialObject]();

      // calculate the local results at each node.
      val resultGroupedPerNode = nodeToPoint.groupByKey().flatMap(x => indexToBCA.get(x._1).get.findBestCatchmentAreas(eps,Math.min(Kprime, K - Ans.size),Ans));

      localAnswers = resultGroupedPerNode.collect().toList.sortBy(_.getScore).reverse

      var roundAnswers = ListBuffer[SpatialObject]()


      val myList = resultGroupedPerNode.collect.toList
      var pos = 0
      var mm = Math.min(Kprime, K - Ans.size)

      while (pos < mm) {
        if (Generic.intersectsList(localAnswers.get(pos), roundAnswers)) {
          break;
        } else {
          val temp = localAnswers.get(pos);
          roundAnswers += temp;

        }
        pos += 1
      }

      Ans = Ans.++(roundAnswers);
      mm = Math.min(Kprime, K - Ans.size)
      iteration = iteration + 1;
    }

    println("\n\n\n");
    println("Final Result");
    println("\n\n\n");

    for (x <- Ans) {
      println(x.getId);
    }

  }

}
