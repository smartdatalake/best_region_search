/* SimpleApp.scala */
package matt.distrib

import matt.ca.{BCAIndexProgressiveOneRound, BCAIndexProgressiveOneRoundRed}
import matt.definitions.{Generic, GridIndexer}
import matt.score.{OneStepResult, ScoreFunctionCount, ScoreFunctionTotalScore}
import matt.{DependencyGraph, POI, SpatialObject}
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.breakable

object OnestepAlgoReduce {

  var topK = 0
  var base = 2
  var gridIndexer: GridIndexer = new GridIndexer(0, 0.0, (0, 0), (0, 0))
  //////////////////////////////////////////////////////
  //////////////////////////////////////////////////////

  def oneStepAlgo(input: (Int, Iterable[POI]), eps: Double, topk: Int, gridIndexer: GridIndexer): (Int, OneStepResult) = {
    val pois: java.util.List[POI] = ListBuffer(input._2.toList: _*)
    val scoreFunction = new ScoreFunctionTotalScore[POI]()
    val bcaFinder = new BCAIndexProgressiveOneRoundRed(true, gridIndexer)
    (input._1, bcaFinder.findBestCatchmentAreas(pois, input._1, eps, topk, scoreFunction))
  }

  def Run(nodeToPoint: RDD[(Int, POI)], eps: Double, topk: Int, gridIndexer2: GridIndexer) {
    this.base = 2
    this.topK = topk
    this.gridIndexer = gridIndexer2
    val Ans = ListBuffer[SpatialObject]()
    var lvl = 0;
    val lvlsOutput = new ListBuffer[RDD[(Int, OneStepResult)]]
    var localAnswers = nodeToPoint.groupByKey().map(x => oneStepAlgo(x, eps, topk, gridIndexer))
    while (width(lvl) != 1) {
      //lvlsOutput.add(lvlsOutput.get(lvl - 1).map(x => mapper(x._1, x._2, base)).groupByKey().map(x => reducer(x._1, x._2, lvl)))
      lvl += 1
      localAnswers = localAnswers.map(x => mapper(x._1, x._2, lvl)).groupByKey().map(x => reducer(x._1, x._2, lvl))
      localAnswers.collect()
      println(lvl)
      //println(gridIndexer.width)
      // this.gridIndexer = new GridIndexer(roundUp(this.gridIndexer.width / base.asInstanceOf[Double]), eps, this.gridIndexer.minmaxLong, this.gridIndexer.minmaxLat)
    }
    // val localAnswers = lvlsOutput.get(lvl - 1).asInstanceOf[RDD[(Int, OneStepResult)]].collect();
    println("***********************************************************************************************************************************")
    localAnswers.collect().foreach(x => x._2.spatialObjects.foreach(x => println(x.getId + ":::::::" + x.getScore)))
    // println(localAnswers.size)
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

  def mapper(index: Int, result: OneStepResult, lvl: Int): (Int, OneStepResult) = {
    val (nodeI, nodeJ) = (((index - 1) / width(lvl).asInstanceOf[Double]).toInt, (index - 1) % width(lvl))
    ((nodeI / base).toInt + (nodeJ / base).toInt * width(lvl) + 1, result)
  }

  def reducer(index: Int, results: Iterable[OneStepResult], lvl: Int): (Int, OneStepResult) = {
    val safe: ListBuffer[SpatialObject] = new ListBuffer[SpatialObject]()
    val unsafe: ListBuffer[SpatialObject] = new ListBuffer[SpatialObject]()
    var I = ((index - 1) % roundUp(width(lvl) / base.asInstanceOf[Double]))
    var J = (((index - 1) / (roundUp(width(lvl) / base)).asInstanceOf[Double]).toInt)
    val cornerALong = I * lvl * gridIndexer.gridSizePerCell * base
    val cornerALat = J * lvl * gridIndexer.gridSizePerCell * base
    val cornerBLong = cornerALong + base * lvl * gridIndexer.gridSizePerCell - 1
    val cornerBLat = cornerALat + base * lvl * gridIndexer.gridSizePerCell - 1
    var candidates = new ListBuffer[SpatialObject]
    results.foreach(x => candidates.addAll(x.spatialObjects))
    candidates = candidates.sortBy(_.getScore).reverse
    var pos = 0
    while (safe.size() < topK && pos < candidates.size) {
      val instance = candidates.get(pos)
      val (cellI, cellJ) = gridIndexer.getCellIndex(instance.getGeometry.getCoordinates.toList(1).x.toFloat
        , instance.getGeometry.getCoordinates.toList(1).y.toFloat)
      if (cellI == cornerALong || cellI == cornerBLong || cellJ == cornerALat || cellJ == cornerBLat)
        unsafe.add(instance)
      else if (Generic.intersectsList(instance, unsafe))
        unsafe.add(instance)
      else if (!Generic.intersectsList(instance, safe))
        safe.add(instance)
      pos += 1
    }
    safe.addAll(unsafe)
    (index, new OneStepResult(cornerALong, cornerALat, cornerBLong, cornerBLat, safe.toList.asInstanceOf[List[SpatialObject]]))
  }

  def roundUp(d: Double) = math.ceil(d).toInt

  def width(lvl: Int): Int = {
    var width = gridIndexer.width
    for (i <- 1 to lvl)
      width = roundUp(width / base.asInstanceOf[Double])
    return width
  }

}