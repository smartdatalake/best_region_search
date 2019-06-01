package matt.definitions

import scala.collection.mutable.ListBuffer

class GridIndexer(val width:Int, val eps:Any,val minmaxLong:(Double,Double),val minmaxLat:(Double,Double)) extends Serializable {
  val dataSize = math.max((minmaxLat._2 - minmaxLat._1), (minmaxLong._2 - minmaxLong._1))
  val cellSize = dataSize * eps.asInstanceOf[Double]
  val dataSizePerCell = math.floor(1 / eps.asInstanceOf[Double]).toInt
  val gridSizePerCell = math.ceil(dataSizePerCell / width.asInstanceOf[Double])

  def getCellIndex(long: Double, lat: Double): (Int, Int) = {
    val cellI = ((long - minmaxLong._1) / cellSize).toInt
    val cellJ = ((minmaxLat._2 - lat) / cellSize).toInt
    return (cellI, cellJ)
  }

  def getNodeIndex(long: Double, lat: Double): Seq[(Int, Int)] = {
    val ((cellI, cellJ), (nodeI, nodeJ)) = getPointIndex(long, lat)
    val result = ListBuffer[(Int, Int)]()
    result += ((nodeI, nodeJ))
    if (nodeI != 0 && nodeJ != 0 && (cellI % gridSizePerCell) == 0 && cellJ % gridSizePerCell == 0) {
      result += ((nodeI - 1, nodeJ))
      result += ((nodeI, nodeJ - 1))
      result += ((nodeI - 1, nodeJ - 1))
    }
    else if (nodeI != 0 && (cellI % gridSizePerCell) == 0)
      result += ((nodeI - 1, nodeJ))
    else if (nodeJ != 0 && cellJ % gridSizePerCell == 0)
      result += ((nodeI, nodeJ - 1))
    result
  }

  def IsOnBorderCell(long: Double, lat: Double): Boolean = {
    val ((cellI, cellJ), (nodeI, nodeJ)) = getPointIndex(long, lat)
    if ((nodeI != 0 && (cellI % gridSizePerCell) == 0) || (nodeJ != 0 && cellJ % gridSizePerCell == 0))
      return true
    if ((nodeI != width - 1 && (cellI % gridSizePerCell) == gridSizePerCell - 1) || (nodeJ != width - 1 && cellJ % gridSizePerCell == gridSizePerCell - 1))
      return true
    return false
  }

  def getPointIndex(long: Double, lat: Double): ((Int, Int), (Int, Int)) = {
    val (cellI, cellJ) = getCellIndex(long, lat)
    val result = ListBuffer[(Int, Int)]()
    return ((cellI, cellJ), (((cellI) / gridSizePerCell.asInstanceOf[Double]).toInt, ((cellJ) / gridSizePerCell.asInstanceOf[Double]).toInt))
  }

  def getNodeNumber(long: Double, lat: Double): Int = {
    val ((cellI, cellJ), (nodeI, nodeJ)) = getPointIndex(long, lat)
    return nodeJ * width + nodeI + 1
  }

  def getNeighborBorderCell(long: Double, lat: Double): Seq[(Int, Int)] = {
    val ((cellI, cellJ), (nodeI, nodeJ)) = getPointIndex(long, lat)
    val result = ListBuffer[(Int, Int)]()
    if (nodeI != 0 && (cellI % gridSizePerCell) == 0) {
      result += ((cellI - 1, cellJ + 1))
      result += ((cellI - 1, cellJ))
      result += ((cellI - 1, cellJ - 1))
    }
    if (nodeJ != 0 && cellJ % gridSizePerCell == 0) {
      result += ((cellI + 1, cellJ - 1))
      result += ((cellI, cellJ - 1))
      result += ((cellI - 1, cellJ - 1))
    }
    if (nodeI != width - 1 && (cellI % gridSizePerCell) == gridSizePerCell - 1) {
      result += ((cellI + 1, cellJ + 1))
      result += ((cellI + 1, cellJ))
      result += ((cellI + 1, cellJ - 1))
    }
    if (nodeJ != width - 1 && cellJ % gridSizePerCell == gridSizePerCell - 1) {
      result += ((cellI + 1, cellJ + 1))
      result += ((cellI, cellJ + 1))
      result += ((cellI - 1, cellJ + 1))
    }
    if (nodeI != 0 && nodeJ != 0 && (cellI % gridSizePerCell) == 0 && cellJ % gridSizePerCell == 0) {
      result += ((cellI - 1, cellJ))
      result += ((cellI, cellJ - 1))
      result += ((cellI - 1, cellJ - 1))
    }
    if (result.size == 6)
      result.remove(5)
    result
  }
}
