/*
/* SimpleApp.scala */
package matt.distrib

import matt.{POI, SpatialObject}
import matt.ca.BCAIndexProgressive2
import matt.definitions.Generic
import matt.score.ScoreFunctionCount
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

object NstepAlgo2 {
  def Run(nodeToPoint: RDD[(Int, POI)], eps: Double, K: Int, width: Int) {
    var Ans = List[SpatialObject]();
    val listOfBCAFinder = nodeToPoint.groupByKey().map(x => (x._1, new BCAIndexProgressive2(ListBuffer(x._2.toList: _*), eps, new ScoreFunctionCount[POI]()))).cache()
  // println("**********************************************************************")
   // nodeToPoint.groupByKey().foreach(x=>println(x._1+":: "+x._2.toList.size))
  //  println(listOfBCAFinder.foreach(x=>println(x._1+" :: "+SizeEstimator.estimate(x._2)/(1024*1024))) )
    var iteration = 0;
    val Kprime = 500;
    while (Ans.length < K) {
      println("Current Iteration: " + iteration);
      // var localAnswers = ListBuffer[SpatialObject]();
      var localAnswers = listOfBCAFinder.flatMap(x => x._2.findBestCatchmentAreas(eps, Kprime, Ans)).collect().toList

      //   for (y <- listOfBCAFinder) {
      //     localAnswers.addAll(y._2.findBestCatchmentAreas(eps, Kprime, Ans))
      //  }
      //   println("**********************************************************************")
      //   println( SizeEstimator.estimate(listOfBCAFinder)/(1024*1024))
      var roundAnswers = ListBuffer[SpatialObject]()
      localAnswers = localAnswers.sortBy(_.getScore).reverse
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
      Ans = Ans.++(roundAnswers);
      iteration = iteration + 1;
    }

    println("\n\n\n");
    println("Final Result in " + iteration + " iteration")
    println("\n\n\n");

    val out=Ans.sortBy(_.getId).reverse
    for (x <- out) {
      println(x.getId);
    }

  }
}
*/
