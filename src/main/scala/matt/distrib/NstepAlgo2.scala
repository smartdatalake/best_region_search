/* SimpleApp.scala */
package matt.distrib

import java.util.PriorityQueue

import collection.mutable.{HashMap, MultiMap, Set}
import matt.{Grid, POI, SpatialObject}
import matt.ca.{BCAIndexProgressive, BCAIndexProgressive2, BCAIndexProgressiveRDD, Block}
import matt.definitions.Generic
import matt.score.ScoreFunctionCount
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

object NstepAlgo2 {

  def Run(nodeToPoint: RDD[(Int, POI)], eps: Double,  K: Int,width:Int) {

    //
    import matt.ca.BCAIndexProgressive
    val bcaFinder = new BCAIndexProgressiveRDD(true)
    var Ans = List[SpatialObject]();
    val indexToGrid:HashMap[Int,Grid]=new HashMap[Int,Grid]()
    val indexToBCA:HashMap[Int,BCAIndexProgressive2]=new HashMap[Int,BCAIndexProgressive2]()
    val indexToQueue:HashMap[Int,PriorityQueue[Block]]=new HashMap[Int,PriorityQueue[Block]]()
  //  nodeToPoint.groupByKey().foreach(x=> indexToBCA.add((x._1,new BCAIndexProgressive2(ListBuffer(x._2.toList: _*), eps, new ScoreFunctionCount[POI]()))))
   val p= nodeToPoint.groupByKey().map(x=>(x._1,new BCAIndexProgressive2(ListBuffer(x._2.toList: _*), eps, new ScoreFunctionCount[POI]()))).cache().collect().toList
    // this has to be iterated (each node has to calculate the best subset)
    var iteration = 0;
    val Kprime =1;
    while (Ans.length < K) {

      println("Current Iteration: " + iteration);
      var localAnswers = ListBuffer[SpatialObject]();

      // calculate the local results at each node.
      //val resultGroupedPerNode = nodeToPoint.groupByKey().flatMap(x => indexToBCA.get(x._1).get.findBestCatchmentAreas(eps,Math.min(Kprime, K - Ans.size),Ans));

      for(y<-p){
        localAnswers.addAll(y._2.findBestCatchmentAreas(eps,Kprime,Ans))
      }
      var roundAnswers = ListBuffer[SpatialObject]()
      localAnswers = localAnswers.sortBy(_.getScore).reverse


     // val myList = resultGroupedPerNode.collect.toList
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
    println("Final Result in " + iteration + " iteration")
    println("\n\n\n");

    for (x <- Ans) {
      println(x.getId);
    }

  }

}
