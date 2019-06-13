package matt.distrib

import matt.ca.{BCAIndexProgressive, BCAIndexProgressiveOneRound}
import matt.definitions.{Generic, GridIndexer}
import matt.score.ScoreFunctionCount
import matt.{BorderResult, POI, SpatialObject}
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.collection.mutable.{HashMap, ListBuffer}

object OnestepAlgoOptimized {

  def oneStepAlgo(input: (Int, Iterable[POI]), eps: Double, topk: Int, gridIndexer: GridIndexer): List[SpatialObject] = {
    val pois: java.util.List[POI] = ListBuffer(input._2.toList: _*)
    val scoreFunction = new ScoreFunctionCount[POI]()
    val (inside, border) = dividePOIs(input, gridIndexer)
    val borderInfo = calBorderTop1(border, eps, gridIndexer).toList
    println(input._1)
    borderInfo.foreach(println)
    val bcaFinder = new BCAIndexProgressiveOneRound(true, gridIndexer)
    bcaFinder.findBestCatchmentAreas(inside,borderInfo,input._1, eps, topk, scoreFunction).asInstanceOf[List[SpatialObject]]
  }

  def Top1BorderAlgo(input: ((Int, Int), Iterable[POI]), eps: Double): ((Int, Int), SpatialObject) = {
    val pois: java.util.List[POI] = ListBuffer(input._2.toList: _*)
    val scoreFunction = new ScoreFunctionCount[POI]();
    val distinct = true;
    val bcaFinder = new BCAIndexProgressive(distinct);
    return (input._1, bcaFinder.findBestCatchmentAreas(pois, eps, 1, scoreFunction).get(0))
  }

  def Run(nodeToPoint: RDD[(Int, POI)], eps: Double, decayConstant: Double, topk: Int, gridIndexer: GridIndexer) {

    val Ans = ListBuffer[SpatialObject]()
    val localAnswers = nodeToPoint.groupByKey().filter(x=>x._2.toList.length>0).flatMap(x => oneStepAlgo(x, eps, topk, gridIndexer))
      .collect.toList.sortBy(_.getScore).reverse
    var pos = 0
    while (Ans.size < topk && pos != localAnswers.size) {
      if (!Generic.intersectsList(localAnswers.get(pos), Ans))
        Ans.add(localAnswers.get(pos))
      pos += 1
    }

    println("\n\n\n");
    println("Final Result");
    println("\n\n\n");

    val out = Ans.sortBy(_.getId).reverse
    for (x <- out) {
      println(x.getId + "   " + x.getScore);
    }
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
      else if ((cellInI > 0 || cellInJ > 0 || cellInI < gridIndexer.gridSizePerCell || cellInJ < gridIndexer.gridSizePerCell)) {
        inside += poi
      }
    }
    return (inside, border)
  }

  def calBorderTop1(poisInCell: HashMap[(Int, Int), ListBuffer[POI]], eps: Double, gridIndexer: GridIndexer): ListBuffer[BorderResult] = {
    val scoreFunction = new ScoreFunctionCount[POI]();
    val bcaFinder = new BCAIndexProgressive(true);
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
}

//First Sub-iteration
////////////////////////////////

/*   val t= borderPoint.groupByKey().map(x => {
      val (right, down, corner) = gridIndexer.get3BorderPartition(x._2)
      var rightBestScore = 0.0
      if (right.size > 0)
        rightBestScore = NstepAlgo.localAlgo(right, eps, 1, List[SpatialObject]()).head.getScore
      var downBestScore = 0.0
      if (down.size > 0)
        downBestScore = NstepAlgo.localAlgo(down, eps, 1, List[SpatialObject]()).head.getScore
      var cornerBestScore = 0.0
      if (corner.size > 0)
        cornerBestScore = NstepAlgo.localAlgo(corner, eps, 1, List[SpatialObject]()).head.getScore
      new BorderResult(x._1, rightBestScore, downBestScore, cornerBestScore)
    }).collect()*/
// t.foreach(println)
/*val localAnswers = nodeToPoint.groupByKey().flatMap(x => {
      var left: Array[Array[Int]] = Array.ofDim[Int](gridIndexer.gridSizePerCell + 1, 2)
      var up: Array[Array[Int]] = Array.ofDim[Int](2, gridIndexer.gridSizePerCell + 1)
      var corner: Array[Array[Int]] = Array.ofDim[Int](2, 2)
      if (borderPoint.contains(x._1 - gridIndexer.width))
        up = borderPoint.get(x._1 - gridIndexer.width).get._1
      if (borderPoint.contains(x._1 - 1))
        left = borderPoint.get(x._1 - 1).get._2
      if (borderPoint.contains(x._1 - gridIndexer.width - 1))
        corner = borderPoint.get(x._1 - gridIndexer.width - 1).get._3
      oneStepAlgo(x, up, left, corner, eps, decayConstant, topk, gridIndexer)
    })
      .collect().toList.sortBy(_.getScore).reverse*/
//Second Sub-iteration
////////////////////////////////