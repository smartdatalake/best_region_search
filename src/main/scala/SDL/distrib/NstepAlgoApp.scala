/* SimpleApp.scala */
package SDL.distrib

import SDL.{POI, SpatialObject}
import SDL.ca.BCAIndexProgressive
import SDL.definitions.{Generic, GridIndexer}
import SDL.score.ScoreFunctionTotalScore
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

object NstepAlgoApp {

  def localAlgo(input: Iterable[POI], eps: Double, topk: Int, finalAnswers: List[SpatialObject], gridIndexer: GridIndexer): List[SpatialObject] = {
    // val scoreFunction = new ScoreFunctionCount[POI]();
    val scoreFunction = new ScoreFunctionTotalScore[POI]();
    val bcaFinder = new BCAIndexProgressive(distinct, gridIndexer);
    return bcaFinder.findBestCatchmentAreas(input.toList, eps, topk, scoreFunction, finalAnswers, -1).toList;
  }

  var distinct = false

  def Run(nodeToPoint: RDD[(Int, POI)], eps: Double, K: Int, sigma: Double, gridIndexer: GridIndexer, distinct: Boolean) {
    var Ans = List[SpatialObject]();
    var iteration = 0;
    val Kprime = K;
    this.distinct = distinct

    while (Ans.length < K) {
      println("Current Iteration: " + iteration);
      // calculate the local results at each node.
      val resultRegionOfRound = nodeToPoint.groupByKey().flatMap(x => localAlgo(x._2, eps, Math.min(Kprime, K - Ans.size), Ans, gridIndexer));
      val localAnswers = resultRegionOfRound.collect().toList.sortBy(_.getScore).reverse
      var roundAnswers = ListBuffer[SpatialObject]()
      /////take Kprime acceptable regions from current round answers as "roundAnswers"
      ////////////////////////////////
      var pos = 0
      var appIndex = 0
      var score = -1.0
      breakable {
        while (pos < Math.min(Kprime, K - Ans.size)) {
          val temp = localAnswers.get(pos)
          if (distinct && Generic.intersectsList(temp, roundAnswers)) {
            if (appIndex == 0) {
              println("oops" + temp.getId + "     " + temp.getScore)
              score = temp.getScore;
              appIndex = roundAnswers.size
            }
            else if (temp.getScore < score * sigma) {
              println("break" + temp.getId + "     " + temp.getScore)
              roundAnswers.remove(appIndex, roundAnswers.size)
              break;
            }
          } else {
            if (score > 0 && (temp.getScore < score * sigma)) {
              println("break2" + temp.getId + "     " + temp.getScore)
              roundAnswers.remove(appIndex, roundAnswers.size)
              break;
            }
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
    val out = Ans.sortBy(_.getScore).reverse
    var totalScore = 0.0
    for (i <- 0 to (K - 1)) {
      totalScore += Ans.get(i).getScore
      println((i + 1) + ":" + Ans.get(i).getId + "     " + Ans.get(i).getScore);
    }
    println("total======" + totalScore)
  }
}
