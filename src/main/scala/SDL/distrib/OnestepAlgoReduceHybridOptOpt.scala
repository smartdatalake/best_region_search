package SDL.distrib

import SDL.ca.{BCAIndexProgressive, BCAIndexProgressiveOneRoundRedHybrid}
import SDL.definitions.{Generic, GridIndexer}
import SDL.score.{OneStepResult, ScoreFunctionTotalScore}
import SDL.{BorderResult, DependencyGraph, POI, SpatialObject}
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.collection.immutable.HashSet
import scala.collection.mutable.{HashMap, ListBuffer}

object OnestepAlgoReduceHybridOptOpt {

  def oneStepAlgo(input: (Int, Iterable[POI]), eps: Double, topk: Int, previous: ListBuffer[SpatialObject], gridIndexer: GridIndexer,ttype:Int): (Int, OneStepResult) = {
    val pois: java.util.List[POI] = ListBuffer(input._2.toList: _*)
    val scoreFunction = new ScoreFunctionTotalScore[POI]()
    val (inside, border) = dividePOIs(input, gridIndexer)
    var borderInfo: List[BorderResult] = null
    if (ttype == 1)
      borderInfo = calBorderTop1(border, eps, gridIndexer).toList
    else if (ttype == 2)
      borderInfo = calBorderUpper(border, eps, gridIndexer).toList
    val bcaFinder = new BCAIndexProgressiveOneRoundRedHybrid(true, gridIndexer)
    (input._1, bcaFinder.findBestCatchmentAreas(inside, borderInfo, input._1, eps, topk, scoreFunction).asInstanceOf[OneStepResult])
  }

  def Run(nodeToPoint: RDD[(Int, POI)], eps: Double, topk: Int, gridIndexer: GridIndexer, base: Int, Kprime: Int,ttype:Int) {
    var Ans = ListBuffer[SpatialObject]()
    var t=0.0
    //  var mapped = new mutable.HashMap[Int, Int]
    //  var used = new mutable.HashMap[Int, Int]
    var round = 1
    var rdds: Array[RDD[(Int, OneStepResult)]] = new Array[RDD[(Int, OneStepResult)]](base * roundUp(math.log(gridIndexer.width) / math.log(base)) + 1)
    t = System.nanoTime()
    rdds(0) = nodeToPoint.groupByKey().map(x => oneStepAlgo(x, eps, Kprime, Ans, gridIndexer,ttype))
    rdds(0).cache()
    //println(rdds(0).map(x=>x._2.countUnsafe+x._2.countSafe).sum())
    //return
    //   println("round:::"+round+ rdds(0).count()+"   time::::" +(System.nanoTime() - t) / 1000000000)

    var lvl=0
    while (Ans.size < topk) {
      lvl = 1;
      // println(rdds(0).count())
      //  println(roundUp(math.log(gridIndexer.width) / math.log(base)))
      while (lvl <= roundUp(math.log(gridIndexer.width) / math.log(base))) {
        var t = System.nanoTime()
        rdds(lvl) = rdds(lvl - 1).map(x => mapper(x._1, x._2, gridIndexer, lvl, base: Int)).groupByKey().map(x => reducer(x._1, x._2, gridIndexer, lvl, base, topk, Ans))
        rdds(lvl).cache()
        System.err.println(lvl + ":::" + rdds(lvl).count())
        // rdds(lvl-1)=null
        lvl += 1
      }
      var t = System.nanoTime()
      val roundResults = rdds(lvl - 1).map(x => x._2).collect().toList.get(0).spatialObjects
    //  System.out.println("dependency size:::"+(rdds(lvl - 1).map(x => x._2).collect().toList.get(0).countUnsafe))
      Ans.addAll(roundResults)
      //   println("collecttime::::" +(System.nanoTime() - t) / 1000000000)

      //println(Ans.sortBy(_.getScore).reverse)
      var topKIndex: HashSet[Int] = HashSet();
      for (spatialObject <- roundResults) {
        topKIndex.+=(spatialObject.getPart);
        topKIndex.+=(spatialObject.getPart-1);
        topKIndex.+=(spatialObject.getPart+1);
        topKIndex.+=(spatialObject.getPart-gridIndexer.width);
        topKIndex.+=(spatialObject.getPart+gridIndexer.width);
      }
      /*gridIndexer.getNodeIndex(spatialObject.getGeometry.getCoordinates.toList(1).x,spatialObject.getGeometry.getCoordinates.toList(1).y)
  .foreach(x =>topKIndex+=x._2*gridIndexer.width+x._1+1)*/
      //   println(topKIndex)
      round += 1

      t = System.nanoTime()
      val partialRoundRDD = nodeToPoint.groupByKey().filter(x => topKIndex.contains(x._1)).map(x => oneStepAlgo(x, eps, Kprime, Ans, gridIndexer,ttype))
      //  println("filterround:::"+round+"   time::::" +(System.nanoTime() - t) / 1000000000+"count:::::"+ partialRoundRDD.count())
      t = System.nanoTime()
      rdds(0) = rdds(0).filter(x => !topKIndex.contains(x._1)).union(partialRoundRDD)
      rdds(0).cache()
      //   println("unionround:::"+round+"   time::::" +(System.nanoTime() - t) / 1000000000+"count:::::"+ rdds(0).count())
      //  System.out.println(rdds(0).count())

      //rdds= new Array[RDD[(Int, OneStepResult)]](base * roundUp(math.log(gridIndexer.width) / math.log(base)) + 1)

    }
    //imple selecting query partition all partition that have less than k' safe region
    //print out frequency of querying partition and icrease for 3rd 4rth round
    Ans = Ans.sortBy(_.getScore).reverse
    System.err.println("SingleHybridOpt," + topk + " eps," + eps)
    System.out.println("{")
    for (i <- 0 to (topk - 1))
      System.out.println("{\n\"rank\":"+(i + 1) + ",\n\"center\":[" + Ans.get(i).getGeometry.getCentroid.getX+","+Ans.get(i).getGeometry.getCentroid.getY + "],\n\"score\":" + Ans.get(i).getScore+"\n}");
    System.out.println("}")
    //  Ans.sortBy(_.getScore).reverse.foreach(x => System.err.println(x.getId + ":::::::" + x.getScore))
  }
  def mapper(index: Int, result: OneStepResult, gridIndexer: GridIndexer, lvl: Int, base: Int): (Int, OneStepResult) = {
    val (nodeI, nodeJ) = ((index - 1) % width(lvl - 1, base: Int, gridIndexer), ((index - 1) / width(lvl - 1, base: Int, gridIndexer).asInstanceOf[Double]).toInt)
    ((nodeI / base).toInt + (nodeJ / base).toInt * width(lvl, base: Int, gridIndexer) + 1, result)
  }

  def reducer(index: Int, results: Iterable[OneStepResult], gridIndexer: GridIndexer, lvl: Int, base: Int, topK: Int,Ans:ListBuffer[SpatialObject]): (Int, OneStepResult) = {
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
      //  if ( !Generic.intersectsList(instance,Ans)) {
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
        //    }
      }
      pos += 1
    }
    (index, new OneStepResult(preSafe, preUnsafe, index, maxMin.toInt, dependencyGraph.getFinalResult()))
  }

  def roundUp(d: Double) = math.ceil(d).toInt

  def width(lvl: Int, base: Int, gridIndexer: GridIndexer): Int = {
    var width = gridIndexer.width
    for (i <- 1 to lvl)
      width = roundUp(width / base.asInstanceOf[Double])
    return width
  }
  def dividePOIs(input: (Int, Iterable[POI]), gridIndexer: GridIndexer): (ListBuffer[POI], HashMap[(Int, Int), ListBuffer[POI]]) = {
    val pois = ListBuffer(input._2.toList: _*)
    val inside = new ListBuffer[POI]
    val border = new HashMap[(Int, Int), ListBuffer[POI]]
    for (poi <- pois) {
      val (cellInI, cellInJ) = gridIndexer.getCellIndexInGrid(input._1, poi.getPoint.getX, poi.getPoint.getY)
      if ((cellInI == -1 || cellInJ == -1 || cellInI == gridIndexer.gridSizePerCell + 1 || cellInJ == gridIndexer.gridSizePerCell + 1)) {
        val t = border.get((cellInI, cellInJ)).getOrElse(new ListBuffer[POI])
        t.+=(poi)
        border.+=(((cellInI, cellInJ), t))
      }
      else if ((cellInI == 0 || cellInJ == 0 || cellInI == gridIndexer.gridSizePerCell || cellInJ == gridIndexer.gridSizePerCell)) {
        inside += poi
        val t = border.get((cellInI, cellInJ)).getOrElse(new ListBuffer[POI])
        t.+=(poi)
        border.+=(((cellInI, cellInJ), t))
      }
      else if ((cellInI > 0 && cellInJ > 0 && cellInI < gridIndexer.gridSizePerCell && cellInJ < gridIndexer.gridSizePerCell)) {
        inside += poi
      }
      else {
        System.err.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
      }
    }
    return (inside, border)
  }

  def calBorderTop1(poisInCell: HashMap[(Int, Int), ListBuffer[POI]], eps: Double, gridIndexer: GridIndexer): ListBuffer[BorderResult] = {
    val scoreFunction = new ScoreFunctionTotalScore[POI]();
    val bcaFinder = new BCAIndexProgressive(true, gridIndexer);
    val output = new ListBuffer[BorderResult]
    for (((cellInI, cellInJ), pois) <- poisInCell) {
      if (cellInI == -1 || cellInI == gridIndexer.gridSizePerCell || cellInJ == -1 || cellInI == gridIndexer.gridSizePerCell) {
        val quadCellPois = new ListBuffer[POI]
        quadCellPois.addAll(poisInCell.get((cellInI, cellInJ)).getOrElse(new ListBuffer[POI]))
        quadCellPois.addAll(poisInCell.get((cellInI + 1, cellInJ)).getOrElse(new ListBuffer[POI]))
        quadCellPois.addAll(poisInCell.get((cellInI + 1, cellInJ + 1)).getOrElse(new ListBuffer[POI]))
        quadCellPois.addAll(poisInCell.get((cellInI, cellInJ + 1)).getOrElse(new ListBuffer[POI]))
        output.add(new BorderResult(cellInI, cellInJ, bcaFinder.findBestCatchmentAreas(quadCellPois, eps, 1, scoreFunction).get(0).getScore))
      }
    }
    return output
  }

  def calBorderUpper(poisInCell: HashMap[(Int, Int), ListBuffer[POI]], eps: Double, gridIndexer: GridIndexer): ListBuffer[BorderResult] = {
    val output = new ListBuffer[BorderResult]
    for (((cellInI, cellInJ), pois) <- poisInCell) {
      if (cellInI == -1 || cellInI == gridIndexer.gridSizePerCell || cellInJ == -1 || cellInI == gridIndexer.gridSizePerCell) {
        var upper=0.0
        poisInCell.get((cellInI, cellInJ)).getOrElse(new ListBuffer[POI]).foreach(x=>upper+=x.getScore)
        poisInCell.get((cellInI+1, cellInJ)).getOrElse(new ListBuffer[POI]).foreach(x=>upper+=x.getScore)
        poisInCell.get((cellInI+1, cellInJ+1)).getOrElse(new ListBuffer[POI]).foreach(x=>upper+=x.getScore)
        poisInCell.get((cellInI, cellInJ+1)).getOrElse(new ListBuffer[POI]).foreach(x=>upper+=x.getScore)
        output.add(new BorderResult(cellInI, cellInJ, upper))
      }
    }
    return output
  }
}