/* SimpleApp.scala */
package SDL.distrib

import SDL.ca.BCAIndexProgressiveOneRound
import SDL.definitions.{Generic, GridIndexer}
import SDL.score.ScoreFunctionTotalScore
import SDL.{POI, SpatialObject}
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

object OnestepAlgo {

  var topK = 0
  //var gridIndexer:GridIndexer=new GridIndexer(0,0,(0,0),(0,0))
  //////////////////////////////////////////////////////
  //////////////////////////////////////////////////////

  def oneStepAlgo(input: (Int, Iterable[POI]), eps: Double, topk: Int, gridIndexer: GridIndexer): List[SpatialObject] = {
    val pois: java.util.List[POI] = ListBuffer(input._2.toList: _*)
    val scoreFunction = new ScoreFunctionTotalScore[POI]()
    val bcaFinder = new BCAIndexProgressiveOneRound(true, gridIndexer)
    bcaFinder.findBestCatchmentAreas(pois, eps, topk, scoreFunction).asInstanceOf[List[SpatialObject]]
  }

  def Run(nodeToPoint: RDD[(Int, POI)], eps: Double, topk: Int, gridIndexer: GridIndexer) {
    this.topK = topk
    //  this.gridIndexer=gridIndexer
    val Ans = ListBuffer[SpatialObject]()
    //val localAnswers = nodeToPoint.groupByKey().flatMap(x => oneStepAlgo(x, eps, topk, gridIndexer))
    //  .collect().toList.sortBy(_.getScore).reverse
    val localAnswers = nodeToPoint.groupByKey().flatMap(x => oneStepAlgo(x, eps, topk, gridIndexer))
      .collect().toList.sortBy(_.getScore).reverse
    println("***********************************************************************************************************************************")
    println(localAnswers.size)
    var pos = 0
    while (Ans.size < topk && pos != localAnswers.size) {
      if (!Generic.intersectsList(localAnswers.get(pos), Ans))
        Ans.add(localAnswers.get(pos))
      pos += 1
    }

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

  //  def mapper(index:Int, graph: DependencyGraph, base:Int):(Int, DependencyGraph)= {
  //  val (nodeI, nodeJ) = gridIndexer.getNodeIndex(index)
  //   ((nodeI / base).toInt + (nodeJ / base).toInt * gridIndexer.width + 1, graph)
  //  }
  // def reducer()
  /*   var temp1 = new ListBuffer[SpatialObject]()
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
   }*/
}