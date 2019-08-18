/* SimpleApp.scala */
package matt.distrib

import java.util

import matt.ca.{BCAIndexProgressiveOneRound, BCAIndexProgressiveOneRoundRed}
import matt.definitions.{Generic, GridIndexer}
import matt.score.{OneStepResult, ScoreFunctionCount, ScoreFunctionTotalScore}
import matt.{DependencyGraph, POI, SpatialObject}
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.breakable

object OnestepAlgoReduce {

  var topK = 300
  var base = 8
  // var gridIndexer: GridIndexer = new GridIndexer(0, 0.0, (0, 0), (0, 0))
  //////////////////////////////////////////////////////
  //////////////////////////////////////////////////////

  def oneStepAlgo(input: (Int, Iterable[POI]), eps: Double, topk: Int, gridIndexer: GridIndexer): (Int, OneStepResult) = {
    val pois: java.util.List[POI] = ListBuffer(input._2.toList: _*)
    val scoreFunction = new ScoreFunctionTotalScore[POI]()
    val bcaFinder = new BCAIndexProgressiveOneRoundRed(true, gridIndexer)
    (input._1, bcaFinder.findBestCatchmentAreas(pois, input._1, eps, topk, scoreFunction))
  }

  def Run(nodeToPoint: RDD[(Int, POI)], eps: Double, topk: Int, gridIndexer: GridIndexer) {
    this.base = 8
    this.topK = topk
    //this.gridIndexer = gridIndexer2
    val Ans = ListBuffer[SpatialObject]()

    var lvl = 1;
    val lvl0 = nodeToPoint.groupByKey().map(x => oneStepAlgo(x, eps, topk, gridIndexer))
    var rdds: Array[RDD[(Int, OneStepResult)]] = new Array[RDD[(Int, OneStepResult)]](base * roundUp(math.log(gridIndexer.width) / math.log(base)) + 1)
    // rdds.
    rdds(0) = nodeToPoint.groupByKey().map(x => oneStepAlgo(x, eps, topk, gridIndexer))
    println(roundUp(math.log(gridIndexer.width) / math.log(base)))
    while (lvl <= roundUp(math.log(gridIndexer.width) / math.log(base))) {
      rdds(lvl) = rdds(lvl - 1).map(x => mapper(x._1, x._2, gridIndexer, lvl)).groupByKey().map(x => reducer(x._1, x._2, gridIndexer, lvl))
      rdds(lvl).cache()
      println(lvl + ":::" + rdds(lvl).count())
      //  rdds(lvl).collect().foreach(x=>x._2.spatialObjects.foreach(x => println(x.getId + ":::::::" + x.getScore)))
      // rdds(lvl-1)=null
      lvl += 1
    }
    /*
    val lvl1=lvl0.map(x => mapper(x._1, x._2, 1)).groupByKey().map(x => reducer(x._1, x._2, 1))
    val lvl2=lvl1.map(x => mapper(x._1, x._2, 2)).groupByKey().map(x => reducer(x._1, x._2, 2))
    val lvl3=lvl2.map(x => mapper(x._1, x._2, 3)).groupByKey().map(x => reducer(x._1, x._2, 3))
    val lvl4=lvl3.map(x => mapper(x._1, x._2, 4)).groupByKey().map(x => reducer(x._1, x._2, 4))
    val lvl5=lvl4.map(x => mapper(x._1, x._2, 5)).groupByKey().map(x => reducer(x._1, x._2, 5))
    val lvl6=lvl5.map(x => mapper(x._1, x._2, 6)).groupByKey().map(x => reducer(x._1, x._2, 6))*/
    /*while (width(lvl) != 1) {
      lvlsOutput.add(lvlsOutput.get(lvl - 1))
      lvl += 1
      localAnswers = localAnswers.map(x => mapper(x._1, x._2, lvl)).groupByKey().map(x => reducer(x._1, x._2, lvl))
      localAnswers.collect()
      println(lvl)
      //println(gridIndexer.width)
      // this.gridIndexer = new GridIndexer(roundUp(this.gridIndexer.width / base.asInstanceOf[Double]), eps, this.gridIndexer.minmaxLong, this.gridIndexer.minmaxLat)
    }*/
    //  rdds(0).collect().foreach(x=>x._2.spatialObjects.foreach(x => System.err.println(x.getId + ":::::::" + x.getScore)))
    // rdds(5).collect().foreach(x=>x._2.spatialObjects.foreach(x => System.err.println(x.getId + ":::::::" + x.getScore)))
    // rdds(14).collect().foreach(x=>x._2.spatialObjects.foreach(x => System.err.println(x.getId + ":::::::" + x.getScore)))
    //rdds(lvl - 1).collect().toList.head._2.spatialObjects.foreach(x => System.err.println(x.getId + ":::::::" + x.getScore))
    val finalResult = rdds(lvl - 1).map(x => x._2).collect().toList.get(0).spatialObjects
    finalResult.asInstanceOf[List[SpatialObject]].foreach(x => System.err.println(x.getId + ":::::::" + x.getScore))
    System.err.println(finalResult.sortBy(_.getScore).reverse)
    //rdds(lvl - 1).collect().to.foreach(x => println(x._2.spatialObjects.toList.size))
    // rdds(3).collect().foreach(x=>x._2.spatialObjects.foreach(x => println(x.getId + ":::::::" + x.getScore)))

    println("***********************************************************************************************************************************")
    //localAnswers.collect().foreach(x => x._2.spatialObjects.foreach(x => println(x.getId + ":::::::" + x.getScore)))
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

    // Ans.sortBy(_.getScore).reverse.foreach(x => println(x.getId + ":::::::" + x.getScore))

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

  def mapper(index: Int, result: OneStepResult, gridIndexer: GridIndexer, lvl: Int): (Int, OneStepResult) = {
    val (nodeI, nodeJ) = ((index - 1) % width(lvl - 1, gridIndexer), ((index - 1) / width(lvl - 1, gridIndexer).asInstanceOf[Double]).toInt)
    ((nodeI / base).toInt + (nodeJ / base).toInt * width(lvl, gridIndexer) + 1, result)
  }

  def reducer(index: Int, results: Iterable[OneStepResult], gridIndexer: GridIndexer, lvl: Int): (Int, OneStepResult) = {
    var preSafe = 0
    var preUnsafe = 0
    var dependencyGraph = new DependencyGraph(gridIndexer)
  //  System.err.println(lvl)
  //  results.foreach(x => System.err.print("pos" + x.index + ","))
    results.foreach(x => {
      preSafe += x.countSafe
      preUnsafe += x.countUnsafe
    })
   // System.err.println("preSafe:::" + preSafe + "      preUnsafe:::::" + preUnsafe)
    var I = ((index - 1) % width(lvl, gridIndexer))
    var J = (((index - 1) / (width(lvl, gridIndexer)).asInstanceOf[Double]).toInt)
    val cornerALong = I * math.pow(base, lvl).toInt * gridIndexer.gridSizePerCell
    val cornerALat = J * math.pow(base, lvl).toInt * gridIndexer.gridSizePerCell
    val cornerBLong = cornerALong + math.pow(base, lvl).toInt * gridIndexer.gridSizePerCell - 1
    val cornerBLat = cornerALat + math.pow(base, lvl).toInt * gridIndexer.gridSizePerCell - 1
    var candidates = new ListBuffer[SpatialObject]
    results.foreach(x => candidates.addAll(x.spatialObjects))
    candidates = candidates.sortBy(_.getScore).reverse
    var pos = 0
    var unsafe = 0
    while (dependencyGraph.safeRegionCnt < topK && pos < candidates.size) {
      val instance = candidates.get(pos)
      val con = dependencyGraph.overlapCon(instance);
      val (cellI, cellJ) = gridIndexer.getCellIndex(instance.getGeometry.getCoordinates.toList(1).x.toFloat
        , instance.getGeometry.getCoordinates.toList(1).y.toFloat)
      if (con == 0 && !(cellI == cornerALong || cellI == cornerBLong || cellJ == cornerALat || cellJ == cornerBLat))
        dependencyGraph.addSafeRegion(instance)
      else if (con == 1) {
        val a = 0
      }
      else if (con == 2 || (cellI == cornerALong || cellI == cornerBLong || cellJ == cornerALat || cellJ == cornerBLat)) {
        dependencyGraph.addUnsafeRegion(instance);
        unsafe += 1
        if (dependencyGraph.IsDependencyIncluded(instance)) {
          // Do not add Safe
          // Do not add M
          var a=0
        } else {
          dependencyGraph.increaseSafeCNT();
          dependencyGraph.addM(instance);
        }
      }
      pos += 1
    }
  //  System.err.println("pos" + index + "    safe::" + (dependencyGraph.safeRegionCnt) + "   unsafe::" + unsafe)
    (index, new OneStepResult(dependencyGraph.safeRegionCnt, unsafe, index, 0, dependencyGraph.getFinalResult()))
  }

  def roundUp(d: Double) = math.ceil(d).toInt

  def width(lvl: Int, gridIndexer: GridIndexer): Int = {
    var width = gridIndexer.width
    for (i <- 1 to lvl)
      width = roundUp(width / base.asInstanceOf[Double])
    return width
  }

}