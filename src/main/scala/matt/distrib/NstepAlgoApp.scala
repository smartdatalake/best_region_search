/* SimpleApp.scala */
package matt.distrib

import java.util

import org.apache.spark.rdd.RDD

import scala.util.control.Breaks._
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import matt.POI
import matt.SpatialObject
import matt.ca.BCAIndexProgressive
import matt.score.{ScoreFunctionCount, ScoreFunctionTotalScore}
import matt.definitions.{Generic, GridIndexer}

object NstepAlgoApp {

  def localAlgo(input:  Iterable[POI], eps: Double, topk: Int, finalAnswers: List[SpatialObject],gridIndexer: GridIndexer): List[SpatialObject] = {
    // val scoreFunction = new ScoreFunctionCount[POI]();
    val scoreFunction = new ScoreFunctionTotalScore[POI]();
    val distinct = true;
    val bcaFinder = new BCAIndexProgressive(distinct,gridIndexer);
    return bcaFinder.findBestCatchmentAreas(input.toList, eps, topk, scoreFunction, finalAnswers).toList;
  }

  def Run(nodeToPoint: RDD[(Int, POI)], eps: Double, K: Int, sigma:Double,gridIndexer: GridIndexer) {
    var Ans = List[SpatialObject]();
    var iteration = 0;
    val Kprime = K;

    while (Ans.length < K) {
      println("Current Iteration: " + iteration);
      // calculate the local results at each node.
      val resultRegionOfRound = nodeToPoint.groupByKey().flatMap(x => localAlgo(x._2, eps, Math.min(Kprime, K - Ans.size), Ans,gridIndexer));
      val localAnswers = resultRegionOfRound.collect().toList.sortBy(_.getScore).reverse
      var roundAnswers = ListBuffer[SpatialObject]()
      /////take Kprime acceptable regions from current round answers as "roundAnswers"
      ////////////////////////////////
      var pos = 0
      var appIndex=0
      breakable {
        while (pos < Math.min(Kprime, K - Ans.size)) {
          if (Generic.intersectsList(localAnswers.get(pos), roundAnswers)) {
            if(appIndex==0){
              val temp = localAnswers.get(pos);
              roundAnswers += temp;
              appIndex=roundAnswers.size-1
            }
            else if(localAnswers.get(pos).getScore<roundAnswers.get(appIndex).getScore*(sigma)) {
              roundAnswers.remove(appIndex,roundAnswers.size)
              break;
            }
          } else {
            val temp = localAnswers.get(pos);
            roundAnswers += temp;
          }
          pos += 1
        }
      }
      ///////////////////////////////////////////////////////////
      //////////////////////////////////////////////////////////
      Ans = Ans.++(roundAnswers);
      iteration = iteration + 1;
    }

     println("\n");
     println("Final Result in " + iteration + " iteration");
     println("\n");
    val out=Ans.sortBy(_.getScore).reverse
    var totalScore=0.0
    for (x <- out) {
      totalScore+=x.getScore
      println(x.getId+"     "+x.getScore);
    }
    println("total======" + totalScore)
  }
}
