/* SimpleApp.scala */
package matt.distrib

import org.apache.spark.rdd.RDD
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import matt.{ POI, SpatialObject}
import matt.ca.BCAIndexProgressiveOneRound
import matt.score.ScoreFunctionCount
import matt.definitions.{Generic, GridIndexer}

object OnestepAlgo {

  def oneStepAlgo(input: (Int, Iterable[POI]), eps: Double, decayConstant: Double, topk: Int, gridIndexer: GridIndexer): List[SpatialObject] = {
    val pois: java.util.List[POI] = ListBuffer(input._2.toList: _*)
    val scoreFunction = new ScoreFunctionCount[POI]()
    val bcaFinder = new BCAIndexProgressiveOneRound(true, gridIndexer)
    bcaFinder.findBestCatchmentAreas(pois, eps, topk, scoreFunction).asInstanceOf[List[SpatialObject]]
  }

  def Run(nodeToPoint: RDD[(Int, POI)], eps: Double, decayConstant: Double, topk: Int, gridIndexer: GridIndexer) {
    val Ans = ListBuffer[SpatialObject]()
    val localAnswers = nodeToPoint.groupByKey().flatMap(x => oneStepAlgo(x, eps, decayConstant, topk, gridIndexer))
      .collect().toList.sortBy(_.getScore).reverse
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