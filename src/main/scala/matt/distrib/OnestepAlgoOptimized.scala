package matt.distrib

import matt.ca.{BCAIndexProgressive, BCAIndexProgressiveOneRound}
import matt.definitions.{Generic, GridIndexer}
import matt.distrib.OnestepAlgo.oneStepAlgo
import matt.score.{ScoreFunctionCount, ScoreFunctionTotalScore}
import matt.{BorderResult, POI, SpatialObject}
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.collection.mutable.{HashMap, ListBuffer}
import scala.util.control.Breaks.{break, breakable}

object OnestepAlgoOptimized {

  var topK=0
  /////////////////////////////////////////////////////////////////////////////////
  /////////////////////////////////////////////////////////////////////////////////

  def oneStepAlgo(input: (Int, Iterable[POI]), eps: Double, topk: Int, gridIndexer: GridIndexer): List[SpatialObject] = {
    val pois: java.util.List[POI] = ListBuffer(input._2.toList: _*)
   // val scoreFunction = new ScoreFunctionCount[POI]()
    val scoreFunction = new ScoreFunctionTotalScore[POI]()
    val (inside, border) = dividePOIs(input, gridIndexer)
    val borderInfo = calBorderTop1(border, eps, gridIndexer).toList
    val bcaFinder = new BCAIndexProgressiveOneRound(true, gridIndexer)
    bcaFinder.findBestCatchmentAreas(inside,borderInfo,input._1, eps, topk, scoreFunction).asInstanceOf[List[SpatialObject]]
  }

  def Top1BorderAlgo(input: ((Int, Int), Iterable[POI]), eps: Double,gridIndexer: GridIndexer): ((Int, Int), SpatialObject) = {
    val pois: java.util.List[POI] = ListBuffer(input._2.toList: _*)
    val scoreFunction = new ScoreFunctionCount[POI]();
    val distinct = true;
    val bcaFinder = new BCAIndexProgressive(distinct,gridIndexer);
    val spatialObject=new SpatialObject();
    spatialObject.setScore(0)
    return (input._1, bcaFinder.findBestCatchmentAreas(pois, eps, 1, scoreFunction).get(0))
  }

  def Run(nodeToPoint: RDD[(Int, POI)], eps: Double, decayConstant: Double, topk: Int, gridIndexer: GridIndexer) {

    this.topK=topk
    val Ans = ListBuffer[SpatialObject]()
    val localAnswers = nodeToPoint.groupByKey().flatMap(x => oneStepAlgo(x, eps, topk, gridIndexer))
      .collect().toList.sortBy(_.getScore).reverse
    // println("***********************************************************************************************************************************")
    // println(localAnswers.size)
    var pos = 0
    while (Ans.size < topk && pos != localAnswers.size) {
      if (!Generic.intersectsList(localAnswers.get(pos), Ans))
        Ans.add(localAnswers.get(pos))
      pos += 1
    }

   // println("\n");
   // println("Final Result");
   // println("\n");

 //   Ans.sortBy(_.getScore).reverse.foreach(x => println(x.getId + ":::::::" + x.getScore))
  }

  def localAnsReducer(a: List[SpatialObject], b: List[SpatialObject]): List[SpatialObject] = {
    var temp1=new ListBuffer[SpatialObject]()
    temp1.addAll(a.toList)
    temp1.addAll(b.toList)
    temp1=temp1.sortBy(_.getScore).reverse
    var pos=0
    val roundAnswers=new ListBuffer[SpatialObject]()
    breakable {
      while (pos < topK&&pos<temp1.size) {
        if (Generic.intersectsList(temp1.get(pos), roundAnswers)) {
          break;
        } else {
          val temp = temp1.get(pos);
          roundAnswers += temp;
        }
        pos += 1
      }
    }
    return roundAnswers.toList

    /*var temp1 = new ListBuffer[SpatialObject]()
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
    return reduceAnswer.toList*/
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
      else{
        System.err.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
      }
    }
    return (inside, border)
  }

  def calBorderTop1(poisInCell: HashMap[(Int, Int), ListBuffer[POI]], eps: Double, gridIndexer: GridIndexer): ListBuffer[BorderResult] = {
    val scoreFunction = new ScoreFunctionCount[POI]();
    val bcaFinder = new BCAIndexProgressive(true,gridIndexer);
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
