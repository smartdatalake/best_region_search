/* SimpleApp.scala */
package SDL.distrib

import SDL.{POI, SpatialObject}
import SDL.ca.BCAIndexProgressive
import SDL.definitions.{Generic, GridIndexer}
import SDL.score.ScoreFunctionTotalScore
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

object NstepAlgo {
  def localAlgo(index: Int, input: Iterable[POI], eps: Double, topk: Int, finalAnswers: List[SpatialObject], gridIndexer: GridIndexer): (Int, List[SpatialObject]) = {
    val scoreFunction = new ScoreFunctionTotalScore[POI]();
    val bcaFinder = new BCAIndexProgressive(distinct, gridIndexer);
    return (index, bcaFinder.findBestCatchmentAreas(input.toList, eps, topk, scoreFunction, finalAnswers, index).toList);
  }

  var distinct = false

  def Run(nodeToPoint: RDD[(Int, POI)], eps: Double, K: Int, Kprime: Int, gridIndexer: GridIndexer, base: Int, dist: Boolean) {
    distinct = dist
    var Ans = List[SpatialObject]();
    var iteration = 0;
    //println(roundUp(math.log(gridIndexer.width) / math.log(base)))
    while (Ans.length < K) {
      // println("Current Iteration: " + iteration+"ANS.size="+Ans.size);
      var lvl = 1;
      var rdds: Array[RDD[(Int, List[SpatialObject])]] = new Array[RDD[(Int, List[SpatialObject])]](base * roundUp(math.log(gridIndexer.width) / math.log(base)) + 1)
      rdds(0) = nodeToPoint.groupByKey().map(x => localAlgo(x._1, x._2, eps, Math.min(Kprime, K - Ans.size), Ans, gridIndexer))
      while (lvl <= roundUp(math.log(gridIndexer.width) / math.log(base))) {
        rdds(lvl) = rdds(lvl - 1).map(x => mapper(x._1, x._2, gridIndexer, lvl, base)).groupByKey().map(x => reducer(x._1, x._2, gridIndexer, Kprime))
        rdds(lvl).cache()
        System.err.println(lvl + ":::" + rdds(lvl).count())
        //  rdds(lvl).collect().foreach(x=>x._2.spatialObjects.foreach(x => println(x.getId + ":::::::" + x.getScore)))
        rdds(lvl - 1) = null
        lvl += 1
      }

      val finalResult = rdds(lvl - 1).map(x => x._2).collect().toList.get(0)
      Ans = Ans.++(finalResult);
      iteration = iteration + 1;
    }

    println("[");
    Ans = Ans.sortBy(_.getScore).reverse
    //System.err.println("Nround,"+K+" eps,"+eps)
    for (i <- 0 to (K - 1)) {
      System.out.println("{\n\"rank\":" + (i + 1) + ",\n\"center\":[" + Ans.get(i).getGeometry.getCentroid.getX + "," + Ans.get(i).getGeometry.getCentroid.getY + "],\n\"score\":" + Ans.get(i).getScore + "\n}")
      if (i != K - 1)
        print(',')
    };
    System.out.println("]")

  }

  def localAnsReducer(a: List[SpatialObject], b: List[SpatialObject], Kprime: Int): List[SpatialObject] = {
    var merged = new ListBuffer[SpatialObject]()
    var minA = 0.0
    if (a.size != 0) minA = a.get(0).getScore
    var minB = 0.0
    if (b.size != 0) minB = b.get(0).getScore
    a.foreach(x => if (x.getScore < minA) minA = x.getScore)
    b.foreach(x => if (x.getScore < minB) minB = x.getScore)
    merged.addAll(a.toList)
    merged.addAll(b.toList)
    merged = merged.sortBy(_.getScore).reverse
    var pos = 0
    val roundAnswers = new ListBuffer[SpatialObject]()
    breakable {
      while (pos < Kprime && pos < merged.size) {
        if ((merged.get(pos).getScore < minA || merged.get(pos).getScore < minB) || Generic.intersectsList(merged.get(pos), roundAnswers)) {
          break;
        } else {
          val temp = merged.get(pos);
          roundAnswers += temp;
        }
        pos += 1
      }
    }
    return roundAnswers.toList
  }

  def mapper(index: Int, result: List[SpatialObject], gridIndexer: GridIndexer, lvl: Int, base: Int): (Int, List[SpatialObject]) = {
    val (nodeI, nodeJ) = ((index - 1) % width(lvl - 1, gridIndexer, base), ((index - 1) / width(lvl - 1, gridIndexer, base).asInstanceOf[Double]).toInt)
    ((nodeI / base).toInt + (nodeJ / base).toInt * width(lvl, gridIndexer, base) + 1, result)
  }

  def reducer(index: Int, results: Iterable[List[SpatialObject]], gridIndexer: GridIndexer, Kprime: Int): (Int, List[SpatialObject]) = {
    var maxMin = 0.0
    var minlocal = 200000.0
    var candidates = new ListBuffer[SpatialObject]
    results.foreach(x => {
      minlocal = 1000000.0
      x.foreach(x => if (x.getScore < minlocal) minlocal = x.getScore())
      candidates.addAll(x)
      if (maxMin < minlocal)
        maxMin = minlocal
    })
    candidates = candidates.sortBy(_.getScore).reverse
    var pos = 0
    val roundAnswers = new ListBuffer[SpatialObject]()
    breakable {
      while (pos < Kprime && pos < candidates.size) {
        if (candidates.get(pos).getScore < maxMin) {
          break;
        } else if (distinct && Generic.intersectsList(candidates.get(pos), roundAnswers)) {
          break;
        } else {
          val temp = candidates.get(pos);
          roundAnswers += temp;
        }
        pos += 1
      }
    }
    (index, roundAnswers.toList)
  }

  def roundUp(d: Double) = math.ceil(d).toInt

  def width(lvl: Int, gridIndexer: GridIndexer, base: Int): Int = {
    var width = gridIndexer.width
    for (i <- 1 to lvl)
      width = roundUp(width / base.asInstanceOf[Double])
    return width
  }
}

/*
  def Run(nodeToPoint: RDD[(Int, POI)], eps: Double, K: Int) {
    var Ans = List[SpatialObject]();
    var iteration = 0;
    val Kprime = K;

    while (Ans.length < K) {
      println("Current Iteration: " + iteration);
      // calculate the local results at each node.
      val resultRegionOfRound = nodeToPoint.groupByKey().flatMap(x => localAlgo(x._2, eps, Math.min(Kprime, K - Ans.size), Ans));
      val localAnswers = resultRegionOfRound.collect().toList.sortBy(_.getScore).reverse
      var roundAnswers = ListBuffer[SpatialObject]()
      /////take Kprime acceptable regions from current round answers as "roundAnswers"
      ////////////////////////////////
      var pos = 0
      breakable {
        while (pos < Math.min(Kprime, K - Ans.size)) {
          if (Generic.intersectsList(localAnswers.get(pos), roundAnswers)) {
            break;
          } else {
            val temp = localAnswers.get(pos);
            roundAnswers += temp;
          }
          pos += 1
        }
      }
      ///////////////////////////////////////////////////////////
      //////////////////////////////////////////////////////////
      Ans = Ans.++(roundAnswers);
      iteration = iteration + 1;
    }

   // println("\n");
   // println("Final Result in " + iteration + " iteration");
   // println("\n");
    //val out=Ans.sortBy(_.getScore).reverse
    //for (x <- out) {
    //  println(x.getId+"     "+x.getScore);
    //}

  }
  def localAnsReducer(a:List[SpatialObject],b:List[SpatialObject],Kprime:Int):List[SpatialObject]={
    var temp1=new ListBuffer[SpatialObject]()
    temp1.addAll(a.toList)
    temp1.addAll(b.toList)
    temp1=temp1.sortBy(_.getScore).reverse
    var pos=0
    val roundAnswers=new ListBuffer[SpatialObject]()
    breakable {
      while (pos < Kprime&&pos<temp1.size) {
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
  }*/
