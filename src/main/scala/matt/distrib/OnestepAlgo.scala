/* SimpleApp.scala */
package matt.distrib

import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import matt.{POI, SpatialObject}
import matt.ca.BCAIndexProgressiveOneRound
import matt.score.ScoreFunctionCount
import matt.definitions.{Generic, GridIndexer}

import scala.util.control.Breaks.{break, breakable}

object OnestepAlgo {

  var topK = 0
  //////////////////////////////////////////////////////
  //////////////////////////////////////////////////////

  def oneStepAlgo(input: (Int, Iterable[POI]), eps: Double, topk: Int, gridIndexer: GridIndexer): List[SpatialObject] = {
    val pois: java.util.List[POI] = ListBuffer(input._2.toList: _*)
    val scoreFunction = new ScoreFunctionCount[POI]()
    val bcaFinder = new BCAIndexProgressiveOneRound(true, gridIndexer)
    bcaFinder.findBestCatchmentAreas(pois, eps, topk, scoreFunction).asInstanceOf[List[SpatialObject]]
  }

  def Run(nodeToPoint: RDD[(Int, POI)], eps: Double, topk: Int, gridIndexer: GridIndexer) {
    this.topK = topk

    val localAnswers = nodeToPoint.groupByKey().map(x => oneStepAlgo(x, eps, topk, gridIndexer)).reduce(localAnsReducer)

    println("\n\n\n");
    println("Final Result");
    println("\n\n\n");

    localAnswers.sortBy(_.getScore).reverse.foreach(x => println(x.getId + ":::::::" + x.getScore))

  }

  def localAnsReducer(a: List[SpatialObject], b: List[SpatialObject]): List[SpatialObject] = {
    var temp1 = new ListBuffer[SpatialObject]()
    temp1.addAll(a.toList)
    temp1.addAll(b.toList)
    temp1 = temp1.sortBy(_.getScore).reverse
    val reduceAnswer = new ListBuffer[SpatialObject]()

    var pos = 0
    breakable {
      while (reduceAnswer.size <= topK && pos < temp1.size) {
        if (!Generic.intersectsList(temp1.get(pos), reduceAnswer)) {
          reduceAnswer += temp1.get(pos);
        }
        pos += 1
      }
    }
    return reduceAnswer.toList
  }
}