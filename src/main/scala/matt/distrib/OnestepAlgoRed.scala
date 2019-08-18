/* SimpleApp.scala */
package matt.distrib

import matt.ca.{BCAIndexProgressiveOneRound, BCAIndexProgressiveOneRoundRed}
import matt.definitions.{Generic, GridIndexer}
import matt.score.{OneStepResult, ScoreFunctionCount, ScoreFunctionTotalScore}
import matt.{DependencyGraph, POI, SpatialObject}
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

object OnestepAlgored {

  var topK = 0
  var gridIndexer:GridIndexer=new GridIndexer(0,0,(0,0),(0,0))
  //////////////////////////////////////////////////////
  //////////////////////////////////////////////////////

  def oneStepAlgo(input: (Int, Iterable[POI]), eps: Double, topk: Int, gridIndexer: GridIndexer): (Int,OneStepResult) = {
    val pois: java.util.List[POI] = ListBuffer(input._2.toList: _*)
    val scoreFunction = new ScoreFunctionTotalScore[POI]()
    val bcaFinder = new BCAIndexProgressiveOneRoundRed(true, gridIndexer)
    (input._1,bcaFinder.findBestCatchmentAreas(pois,input._1, eps, topk, scoreFunction))
  }

  def Run(nodeToPoint: RDD[(Int, POI)], eps: Double, topk: Int, gridIndexer: GridIndexer) {
    val base=2
    this.topK = topk
    this.gridIndexer=gridIndexer
    val Ans = ListBuffer[SpatialObject]()
   val localAnswers = nodeToPoint.groupByKey().map(x => oneStepAlgo(x, eps, topk, gridIndexer)).map(x=>mapper(x._1,x._2,base)).groupByKey().map(x=>reducer(x._1,x._2)).collect()
     println("***********************************************************************************************************************************")
     println(localAnswers.size)
    var pos = 0
    /*while (Ans.size < topk && pos != localAnswers.size) {
      if (!Generic.intersectsList(localAnswers.get(pos), Ans))
        Ans.add(localAnswers.get(pos))
      pos += 1
    }*/

  //  println("\n");
  //  println("Final Result");
   // println("\n");

    Ans.sortBy(_.getScore).reverse.foreach(x => println(x.getId + ":::::::" + x.getScore))

  }

  def localAnsReducer(a: List[SpatialObject], b: List[SpatialObject]): List[SpatialObject] = {
    var temp1 = new ListBuffer[SpatialObject]()
    temp1.addAll(a.toList)
    temp1.addAll(b.toList)
    temp1 = temp1.sortBy(_.getScore).reverse
    var pos = 0
    val roundAnswers = new ListBuffer[SpatialObject]()
    while (roundAnswers.size < topK && pos < temp1.size) {
      if (!Generic.intersectsList(temp1.get(pos), roundAnswers)) {
        val temp = temp1.get(pos);
        roundAnswers += temp;
      }
      pos += 1
    }
    return roundAnswers.toList
  }
  def mapper( index:Int,result: OneStepResult, base:Int):(Int, OneStepResult)= {
    val (nodeI, nodeJ) = gridIndexer.getNodeIndex(index)
    ((nodeI / base).toInt + (nodeJ / base).toInt * gridIndexer.width + 1, result)
  }

  def reducer(index:Int, results:Iterable[OneStepResult]):(Int,OneStepResult)= {
    var cornerALong = 2000000000
    var cornerALat = 2000000000
    var cornerBLong = -10
    var cornerBLat = -10
    val safe: OneStepResult = new OneStepResult()
    val unsafe: OneStepResult = new OneStepResult()


    var candidates = new ListBuffer[SpatialObject]
    results.foreach(x => candidates.addAll(x.spatialObjects))
    candidates = candidates.sortBy(_.getScore).reverse
    var pos = 0
    while (safe.spatialObjects.size() <= topK && pos < candidates.size) {
      val instance = candidates.get(pos)
      val (cellI, cellJ) = gridIndexer.getCellIndex(instance.getGeometry.getCoordinates.toList(1).x.toFloat
        , instance.getGeometry.getCoordinates.toList(1).y.toFloat)
      if (cellI == cornerALong || cellI == cornerBLong || cellJ == cornerALat || cellJ == cornerBLat)
        unsafe.spatialObjects.add(instance)
      else if (Generic.intersectsList(instance, unsafe.spatialObjects.asInstanceOf[ListBuffer[SpatialObject]]))
        unsafe.spatialObjects.add(instance)
      else if (!Generic.intersectsList(instance, safe.spatialObjects.asInstanceOf[ListBuffer[SpatialObject]]))
        safe.spatialObjects.add(instance)
    }
    safe.countUnsafe = cornerALat
    safe.index = cornerBLong
    safe.cornerBLat = cornerBLat
    safe.spatialObjects.addAll(unsafe.spatialObjects)
    (index, safe)
  }

}