/* SimpleApp.scala */
package matt.distrib

import org.apache.spark.rdd.RDD
import scala.util.control.Breaks._
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import matt.POI;
import matt.SpatialObject;
import matt.ca.BCAIndexProgressive;
import matt.score.ScoreFunctionCount;
import matt.definitions.Generic

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

  def Run(nodeToPoint: RDD[(Int, POI)], eps: Double,  K: Int) {

    //
    var Ans = List[SpatialObject]();

    // this has to be iterated (each node has to calculate the best subset)
    var iteration = 0;
    val Kprime =1;
    while (Ans.length < K) {

      println("Current Iteration: " + iteration);
      var localAnswers = List[SpatialObject]();

      // calculate the local results at each node.
      val resultGroupedPerNode = nodeToPoint.groupByKey().flatMap(x => localAlgo(x, eps, Math.min(Kprime, K - Ans.size), Ans));

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
    println("Final Result in "+iteration+" iteration");
    println("\n\n\n");

    for (x <- Ans) {
      println(x.getId);
    }

  }
}
