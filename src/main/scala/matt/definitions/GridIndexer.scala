package matt.definitions

import scala.collection.mutable.ListBuffer

class GridIndexer(val width:Int, val eps:Any,val minmaxLong:(Float,Float),val minmaxLat:(Float,Float)) extends Serializable {
  val dataSize = math.max((minmaxLat._2 - minmaxLat._1), (minmaxLong._2 - minmaxLong._1))
  val gridSize = dataSize / width
  val cellSize = gridSize * eps.asInstanceOf[Double]

  def getCellIndex(long: Float, lat: Float): (Int, Int) = {
    val nodeIndex = getNodeIndex(long, lat).head
    val cellI = ((long - nodeIndex._1 * gridSize) / cellSize).toInt
    val cellJ = ((minmaxLat._2 - nodeIndex._2 * gridSize - lat) / cellSize).toInt
    return (cellI, cellJ)
  }

  def getNodeIndex(long: Float, lat: Float): Seq[(Int, Int)] = {
    val result = ListBuffer[(Int, Int)]()
    val nodeI = ((long - minmaxLong._1) / gridSize).toInt
    val nodeJ = ((minmaxLat._2 - lat) / gridSize).toInt
    result.+=((nodeI, nodeJ))
    if (nodeI != 0 && nodeJ != 0 && long < minmaxLong._1 + gridSize * nodeI + cellSize && lat > minmaxLat._2 - (nodeJ) * gridSize - cellSize) {
      result.+=((nodeI + 1, nodeJ))
      result += ((nodeI, nodeJ + 1))
      result += ((nodeI + 1, nodeJ + 1))
    }
    else if (nodeI != 0 && long < minmaxLong._1 + gridSize * nodeI + cellSize)
      result.+=((nodeI + 1, nodeJ))
    else if (nodeJ != 0 && lat > minmaxLat._2 - (nodeJ) * gridSize - cellSize)
      result += ((nodeI, nodeJ + 1))
    result
  }

  def getNodeNumber(long: Float, lat: Float): Int = {
    val (nodeI, nodeJ) = getNodeIndex(long, lat).head
    return nodeJ * width + nodeI + 1
  }
}
