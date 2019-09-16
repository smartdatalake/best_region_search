package matt.distrib

import matt.{DependencyGraph, POI, SpatialObject}
import matt.ca.BCAIndexProgressiveOneRoundRedHybrid
import matt.definitions.GridIndexer
import matt.score.{OneStepResult, ScoreFunctionTotalScore}
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

object OnestepAlgoReduceHybrid {

  def oneStepAlgo(input: (Int, Iterable[POI]), eps: Double, topk: Int, previous: ListBuffer[SpatialObject], gridIndexer: GridIndexer): (Int, OneStepResult) = {
    val pois: java.util.List[POI] = ListBuffer(input._2.toList: _*)
    val scoreFunction = new ScoreFunctionTotalScore[POI]()
    val bcaFinder = new BCAIndexProgressiveOneRoundRedHybrid(true, gridIndexer)
    (input._1, bcaFinder.findBestCatchmentAreas(pois, input._1, eps, topk, scoreFunction,previous))
  }

  def Run(nodeToPoint: RDD[(Int, POI)], eps: Double, topk: Int, gridIndexer: GridIndexer, base: Int,Kprime:Int) {
    var Ans = ListBuffer[SpatialObject]()
    while (Ans.size < topk) {
      var lvl = 1;
      val rdds: Array[RDD[(Int, OneStepResult)]] = new Array[RDD[(Int, OneStepResult)]](base * roundUp(math.log(gridIndexer.width) / math.log(base)) + 1)
      rdds(0) = nodeToPoint.groupByKey().map(x => oneStepAlgo(x, eps, Kprime, Ans, gridIndexer))
      println(roundUp(math.log(gridIndexer.width) / math.log(base)))
      while (lvl <= roundUp(math.log(gridIndexer.width) / math.log(base))) {
        rdds(lvl) = rdds(lvl - 1).map(x => mapper(x._1, x._2, gridIndexer, lvl, base: Int)).groupByKey().map(x => reducer(x._1, x._2, gridIndexer, lvl, base, topk))
        rdds(lvl).cache()
        println(lvl + ":::" + rdds(lvl).count())
        rdds(lvl-1)=null
        lvl += 1
      }
      Ans.addAll(rdds(lvl - 1).map(x => x._2).collect().toList.get(0).spatialObjects)
      Ans=Ans.sortBy(_.getScore).reverse
     // System.err.println("SingleHybrid,"+topk+" eps,"+eps)
 //     for (i<- 0 to Ans.size-1) {
 //       System.err.println((i+1)+":"+Ans.get(i).getId+"     "+Ans.get(i).getScore);

  //    }
    }
    Ans=Ans.sortBy(_.getScore).reverse
    System.err.println("SingleHybrid,"+topk+" eps,"+eps)
    for (i<- 0 to (topk-1)) {
      System.err.println((i+1)+":"+Ans.get(i).getId+"     "+Ans.get(i).getScore);

    }
  //  Ans.sortBy(_.getScore).reverse.foreach(x => System.err.println(x.getId + ":::::::" + x.getScore))
  }

  def mapper(index: Int, result: OneStepResult, gridIndexer: GridIndexer, lvl: Int, base: Int): (Int, OneStepResult) = {
    val (nodeI, nodeJ) = ((index - 1) % width(lvl - 1, base: Int, gridIndexer), ((index - 1) / width(lvl - 1, base: Int, gridIndexer).asInstanceOf[Double]).toInt)
    ((nodeI / base).toInt + (nodeJ / base).toInt * width(lvl, base: Int, gridIndexer) + 1, result)
  }

  def reducer(index: Int, results: Iterable[OneStepResult], gridIndexer: GridIndexer, lvl: Int, base: Int, topK: Int): (Int, OneStepResult) = {
    var preSafe = 0
    var preUnsafe = 0
    val dependencyGraph = new DependencyGraph(gridIndexer)
    results.foreach(x => {
      preSafe += x.countSafe
      preUnsafe += x.countUnsafe
    })
    val I = ((index - 1) % width(lvl, base: Int, gridIndexer))
    val J = (((index - 1) / (width(lvl, base: Int, gridIndexer)).asInstanceOf[Double]).toInt)
    val cornerALong = I * math.pow(base, lvl).toInt * gridIndexer.gridSizePerCell
    val cornerALat = J * math.pow(base, lvl).toInt * gridIndexer.gridSizePerCell
    val cornerBLong = cornerALong + math.pow(base, lvl).toInt * gridIndexer.gridSizePerCell - 1
    val cornerBLat = cornerALat + math.pow(base, lvl).toInt * gridIndexer.gridSizePerCell - 1
    var maxMin = 0.0
    var minlocal = 200000.0
    results.foreach(x => {
    if(maxMin<x.minSafe)
      maxMin=x.minSafe
    })
    var candidates = new ListBuffer[SpatialObject]
    results.foreach(x => candidates.addAll(x.spatialObjects))
    candidates = candidates.sortBy(_.getScore).reverse
    var pos = 0
    var unsafe = 0
    while (dependencyGraph.safeRegionCnt < topK && pos < candidates.size && candidates.get(pos).getScore >= maxMin) {
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
          var a = 0
        } else {
          dependencyGraph.increaseSafeCNT();
          dependencyGraph.addM(instance);
        }
      }
      pos += 1
    }
    (index, new OneStepResult(dependencyGraph.safeRegionCnt, unsafe, index,maxMin.toInt, dependencyGraph.getFinalResult()))
  }

  def roundUp(d: Double) = math.ceil(d).toInt

  def width(lvl: Int, base: Int, gridIndexer: GridIndexer): Int = {
    var width = gridIndexer.width
    for (i <- 1 to lvl)
      width = roundUp(width / base.asInstanceOf[Double])
    return width
  }

}