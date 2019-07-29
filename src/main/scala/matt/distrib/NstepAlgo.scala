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

  def localAlgo(input:  Iterable[POI], eps: Double, topk: Int, finalAnswers: List[SpatialObject]): List[SpatialObject] = {
    val scoreFunction = new ScoreFunctionCount[POI]();
    val distinct = true;
    val bcaFinder = new BCAIndexProgressive(distinct);
    val t=System.nanoTime();

    return bcaFinder.findBestCatchmentAreas(input.toList, eps, topk, scoreFunction, finalAnswers).toList;
  }

  def Run(nodeToPoint: RDD[(Int, POI)], eps: Double, K: Int) {
    var Ans = List[SpatialObject]();
    var iteration = 0;
    val Kprime = K;

    while (Ans.length < K) {
      println("Current Iteration: " + iteration);
      // calculate the local results at each node.
      val resultRegionOfRound = nodeToPoint.groupByKey().flatMap(x => localAlgo(x._2, eps, Math.min(Kprime, K - Ans.size), Ans));
      val localAnswers = resultRegionOfRound.collect().toList.sortBy(_.getScore).reverse
      var roundAnswers = ListBuffer[SpatialObject]()
      /////take Kprime acceptable regions from current round answers as "roundAnswers"
      ////////////////////////////////
      var pos = 0
      breakable {
        while (pos < Math.min(Kprime, K - Ans.size)) {
          if (Generic.intersectsList(localAnswers.get(pos), roundAnswers)) {
            break;
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
  //  val out=Ans.sortBy(_.getScore).reverse
   // for (x <- out) {
    //  println(x.getId+"     "+x.getScore);
   // }

  }
  def localAnsReducer(a:List[SpatialObject],b:List[SpatialObject],Kprime:Int):List[SpatialObject]={
    var temp1=new ListBuffer[SpatialObject]()
    temp1.addAll(a.toList)
    temp1.addAll(b.toList)
    temp1=temp1.sortBy(_.getScore).reverse
    var pos=0
    val roundAnswers=new ListBuffer[SpatialObject]()
    breakable {
      while (pos < Kprime&&pos<temp1.size) {
        if (Generic.intersectsList(temp1.get(pos), roundAnswers)) {
          break;
        } else {
          val temp = temp1.get(pos);
          roundAnswers += temp;
        }
        pos += 1
      }
    }
    return roundAnswers.toList
  }
}
