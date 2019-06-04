/* SimpleApp.scala */
package matt.distrib

import matt.ca.{BCAIndexProgressiveOneRound, BCAIndexProgressiveOneRoundOptimized}
import matt.definitions.{Generic, GridIndexer}
import matt.score.ScoreFunctionCount
import matt.{BorderResult, POI, SpatialObject}
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.collection.immutable.HashMap
import scala.collection.mutable.ListBuffer

object OnestepAlgoOptimized {

  def oneStepAlgo(input: (Int, Iterable[POI]), borderInfo: HashMap[Int,BorderResult], eps: Double
                  , decayConstant: Double, topk: Int, gridIndexer: GridIndexer): List[SpatialObject] = {
    val pois: java.util.List[POI] = ListBuffer(input._2.toList: _*)
    val scoreFunction = new ScoreFunctionCount[POI]()
    val bcaFinder = new BCAIndexProgressiveOneRound(true, gridIndexer)
    bcaFinder.findBestCatchmentAreas(pois, input._1, borderInfo, eps, topk, scoreFunction).asInstanceOf[List[SpatialObject]]
  }

  def Run(nodeToPoint: RDD[(Int, POI)], borderPoint: RDD[(Int, POI)], eps: Double, decayConstant: Double, topk: Int, gridIndexer: GridIndexer) {
    val Ans = ListBuffer[SpatialObject]()
    //First Sub-iteration
    ////////////////////////////////

    val t=new HashMap[Int,BorderResult]()
    borderPoint.groupByKey().map(x => {
      val (right, down, corner) = gridIndexer.get3BorderPartition(x._2)
      val rightBestScore = NstepAlgo.localAlgo(right, eps, 1, List[SpatialObject]()).head.getScore
      val downBestScore = NstepAlgo.localAlgo(down, eps, 1, List[SpatialObject]()).head.getScore
      val cornerBestScore = NstepAlgo.localAlgo(corner, eps, 1, List[SpatialObject]()).head.getScore
      return new HashMap[Int, BorderResult]().add((x._1, new BorderResult(rightBestScore, downBestScore, cornerBestScore)))
    }).foreach(x=>t.add(x))


    //Second Sub-iteration
    ////////////////////////////////
    val localAnswers = nodeToPoint.groupByKey().flatMap(x => oneStepAlgo(x, t, eps, decayConstant, topk, gridIndexer))
      .collect().toList.sortBy(_.getScore).reverse
    // println("***********************************************************************************************************************************")
    // println(localAnswers.size)
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