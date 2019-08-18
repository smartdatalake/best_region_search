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

object NstepAlgo {

  def localAlgo(input:  Iterable[POI], eps: Double, topk: Int, finalAnswers: List[SpatialObject],gridIndexer: GridIndexer): List[SpatialObject] = {
   // val scoreFunction = new ScoreFunctionCount[POI]();
    val scoreFunction = new ScoreFunctionTotalScore[POI]();
    val distinct = true;
    val bcaFinder = new BCAIndexProgressive(distinct,gridIndexer);
    return bcaFinder.findBestCatchmentAreas(input.toList, eps, topk, scoreFunction, finalAnswers).toList;
  }

  def Run(nodeToPoint: RDD[(Int, POI)], eps: Double, K: Int,gridIndexer: GridIndexer) {
    var Ans = List[SpatialObject]();
    var iteration = 0;
    val Kprime = K;

    while (Ans.length < K) {
      println("Current Iteration: " + iteration);
      // calculate the local results at each node.
      val resultRegionOfRound = nodeToPoint.groupByKey().map(x => localAlgo(x._2, eps, Math.min(Kprime, K - Ans.size), Ans,gridIndexer )).reduce((a,b)=>localAnsReducer(a,b,Kprime)).sortBy(_.getScore).reverse;
      /////take Kprime acceptable regions from current round answers as "roundAnswers"
      ////////////////////////////////
      ///////////////////////////////////////////////////////////
      //////////////////////////////////////////////////////////
      System.err.println(resultRegionOfRound)
      Ans = Ans.++(resultRegionOfRound);
      iteration = iteration + 1;
    }

    // println("\n");
    println("Final Result in " + iteration + " iteration");
    // println("\n");
    val out=Ans.sortBy(_.getScore).reverse
    var totalScore=0.0
   for (x <- out) {
     totalScore+=x.getScore
    // println(x.getId+"     "+x.getScore);
    }
    println("total======" + totalScore)
  }
  def localAnsReducer(a:List[SpatialObject],b:List[SpatialObject],Kprime:Int):List[SpatialObject]={
    var merged=new ListBuffer[SpatialObject]()
    var minA=0.0
    if(a.size!=0) minA = a.get(0).getScore
    var minB=0.0
    if(b.size!=0) minB = b.get(0).getScore
    a.foreach(x=>if (x.getScore < minA) minA=x.getScore)
    b.foreach(x=>if (x.getScore < minB) minB=x.getScore)
    merged.addAll(a.toList)
    merged.addAll(b.toList)
    merged=merged.sortBy(_.getScore).reverse
    var pos=0
    val roundAnswers=new ListBuffer[SpatialObject]()
    breakable {
      while (pos < Kprime&&pos<merged.size) {
        if ((merged.get(pos).getScore<minA || merged.get(pos).getScore<minB) ||  Generic.intersectsList(merged.get(pos), roundAnswers)) {
          break;
        } else {
          val temp = merged.get(pos);
          roundAnswers += temp;
        }
        pos += 1
      }
    }
    return roundAnswers.toList
  }
}
/*
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

   // println("\n");
   // println("Final Result in " + iteration + " iteration");
   // println("\n");
    //val out=Ans.sortBy(_.getScore).reverse
    //for (x <- out) {
    //  println(x.getId+"     "+x.getScore);
    //}

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
  }*/
