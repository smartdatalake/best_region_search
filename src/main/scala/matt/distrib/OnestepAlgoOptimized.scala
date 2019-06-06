/* SimpleApp.scala */
package matt.distrib
import matt.ca.{BCAIndexProgressiveOneRound}
import matt.definitions.{Generic, GridIndexer}
import matt.score.ScoreFunctionCount
import matt.{ POI, SpatialObject}
import org.apache.spark.rdd.RDD
import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer

object OnestepAlgoOptimized {

  def oneStepAlgo(input: (Int, Iterable[POI]), up: Array[Array[Int]], left: Array[Array[Int]], corner: Array[Array[Int]], eps: Double
                  , decayConstant: Double, topk: Int, gridIndexer: GridIndexer): List[SpatialObject] = {
    val pois: java.util.List[POI] = ListBuffer(input._2.toList: _*)
    val scoreFunction = new ScoreFunctionCount[POI]()
    val bcaFinder = new BCAIndexProgressiveOneRound(true, gridIndexer)
    bcaFinder.findBestCatchmentAreas(pois,input._1,up,left,corner, eps, topk,scoreFunction).asInstanceOf[List[SpatialObject]]
  }

  def Run(nodeToPoint: RDD[(Int, POI)], borderPoint: HashMap[Int, (Array[Array[Int]], Array[Array[Int]], Array[Array[Int]])], eps: Double, decayConstant: Double, topk: Int, gridIndexer: GridIndexer) {
    val Ans = ListBuffer[SpatialObject]()

    val localAnswers = nodeToPoint.groupByKey().flatMap(x => {
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

    val out = Ans.sortBy(_.getId).reverse
    for (x <- out) {
      println(x.getId+"   "+x.getScore);
    }

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

//Second Sub-iteration
////////////////////////////////