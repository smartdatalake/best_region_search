package matt.definitions

import matt.{POI, SpatialObject}
import org.locationtech.jts.geom.Point

import scala.collection.mutable.ListBuffer

class GridIndexer(val width:Int, val eps:Any,val minmaxLong:(Double,Double),val minmaxLat:(Double,Double)) extends Serializable {
  val dataSize = math.max((minmaxLat._2 - minmaxLat._1), (minmaxLong._2 - minmaxLong._1))
  val cellSize = eps.asInstanceOf[Double]
  val p = cellSize / dataSize.asInstanceOf[Double]
  val dataSizePerCell = math.floor(dataSize / cellSize.asInstanceOf[Double]).toInt
  val gridSizePerCell = math.ceil(dataSizePerCell / width.asInstanceOf[Double]).toInt

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

  def getNodeOptIndex(long: Double, lat: Double): Seq[(Int, Int)] = {
    val ((cellI, cellJ), (nodeI, nodeJ)) = getPointIndex(long, lat)
    val (cellInI, cellInJ) = getCellIndexInGrid(nodeJ * width + nodeI + 1, long, lat)
    val result = ListBuffer[(Int, Int)]()
    result += ((nodeI, nodeJ))
    if (nodeI != 0 && (cellInI == 0 || cellInI == 1)) {
      result += ((nodeI - 1, nodeJ))
      //result += ((nodeI, nodeJ - 1))
      //result += ((nodeI - 1, nodeJ - 1))
    }
    if (nodeJ != 0 && (cellInJ == 0 || cellInJ == 1))
      result += ((nodeI, nodeJ - 1))
    if (nodeI != width - 1 && cellInI == gridSizePerCell - 1)
      result += ((nodeI + 1, nodeJ))
    if (nodeJ != width - 1 && cellInJ == gridSizePerCell - 1)
      result += ((nodeI, nodeJ + 1))
    if ((nodeI != 0 && (cellInI == 0 || cellInI == 1)) && (nodeJ != 0 && (cellInJ == 0 || cellInJ == 1)))
      result += ((nodeI - 1, nodeJ - 1))
    if ((nodeI != 0 && (cellInI == 0 || cellInI == 1)) && (nodeJ != width - 1 && cellInJ == gridSizePerCell - 1))
      result += ((nodeI - 1, nodeJ + 1))
    if ( (nodeI != width - 1 && cellInI == gridSizePerCell - 1) && (nodeJ != width - 1 && (cellInJ == 0 || cellInJ == 1)))
      result += ((nodeI + 1, nodeJ - 1))
    if ((nodeI != width - 1 && cellInI == gridSizePerCell - 1) && (nodeJ != width - 1 && cellInJ == gridSizePerCell - 1))
      result += ((nodeI + 1, nodeJ + 1))
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

  def getNodeNumber_Border(long: Double, lat: Double): Int = {
    val ((cellI, cellJ), (nodeI, nodeJ)) = getPointIndex(long, lat)
    if (nodeI != 0 && (cellI % gridSizePerCell) == 0)
      return nodeJ * width + nodeI
    if (nodeJ != 0 && cellJ % gridSizePerCell == 0)
      return (nodeJ - 1) * width + nodeI + 1
    if (nodeJ != width - 1 && cellJ % gridSizePerCell == gridSizePerCell - 1)
      return nodeJ * width + nodeI + 1
    if (nodeI != width - 1 && (cellI % gridSizePerCell) == gridSizePerCell - 1)
      return nodeJ * width + nodeI + 1
    return -1
  }

  def getNodeNumber_Pos_Border(long: Double, lat: Double): Seq[(Int, Boolean)] = {
    val result = new ListBuffer[(Int, Boolean)]()
    val ((cellI, cellJ), (nodeI, nodeJ)) = getPointIndex(long, lat)
    if (nodeI != 0 && (cellI % gridSizePerCell) == 0)
      result += ((nodeJ * width + nodeI, true))
    if (nodeJ != 0 && cellJ % gridSizePerCell == 0)
      result += (((nodeJ - 1) * width + nodeI + 1, false))
    if (nodeJ != width - 1 && cellJ % gridSizePerCell == gridSizePerCell - 1)
      result += ((nodeJ * width + nodeI + 1, true))
    if (nodeI != width - 1 && (cellI % gridSizePerCell) == gridSizePerCell - 1)
      result += ((nodeJ * width + nodeI + 1, false))
    if ((nodeJ != 0 && cellJ % gridSizePerCell == 0) && (nodeI != 0 && (cellI % gridSizePerCell) == 0)) {
      result += (((nodeJ - 1) * width + nodeI, false))
      result += (((nodeJ - 1) * width + nodeI, true))
    }
    return result
  }

  def get3BorderPartition(node:Int,pois: Iterable[POI]): Seq[((Int,Int),Iterable[POI])] = {
    val right = new ListBuffer[POI]()
    val down = new ListBuffer[POI]()
    val corner = new ListBuffer[POI]()
    val result=new ListBuffer[((Int,Int),Iterable[POI])]()
    for (poi <- pois) {
      val (cellIin,cellJin)=getCellIndexInGrid(node,poi)
      if ((cellIin==gridSizePerCell) || (cellIin==gridSizePerCell-1))
        right += (poi)
      if ((cellJin==gridSizePerCell) || (cellJin==gridSizePerCell-1))
        down += (poi)
      if (((cellIin==gridSizePerCell) || (cellIin==gridSizePerCell-1))
        && ((cellJin==gridSizePerCell) || (cellJin==gridSizePerCell-1)))
        corner += (poi)
    }
  /*  for (poi <- pois.toList) {
      val (cellI,cellJ) =getCellIndexInGrid(node, poi.getPoint.getX, poi.getPoint.getY)
      if (cellI==gridSizePerCell||cellI==gridSizePerCell-1)
        right(cellJ)(cellI-gridSizePerCell+1)+=1
      if (cellJ==gridSizePerCell||cellJ==gridSizePerCell-1)
        down(cellJ-gridSizePerCell+1)(cellI)+=1
      if (((cellI==gridSizePerCell||cellI==gridSizePerCell-1))&&((cellJ==gridSizePerCell||cellJ==gridSizePerCell-1)))
        corner(cellJ-gridSizePerCell+1)(cellI-gridSizePerCell+1)+=1
    }*/
    result+=(((node,0),right))
    result+=(((node,1),down))
    result+=(((node,2),corner))
    return result
  }

  def IsOnBorderLeft(long: Double, lat: Double): Boolean = {
    val ((cellI, cellJ), (nodeI, nodeJ)) = getPointIndex(long, lat)
    if (nodeI != 0 && (cellI % gridSizePerCell) == 0)
      return true
    return false
  }

  def IsOnBorderUp(long: Double, lat: Double): Boolean = {
    val ((cellI, cellJ), (nodeI, nodeJ)) = getPointIndex(long, lat)
    if (nodeJ != 0 && (cellJ % gridSizePerCell) == 1)
      return true
    return false
  }

  def IsOnBorderCorner(long: Double, lat: Double): Boolean = {
    val ((cellI, cellJ), (nodeI, nodeJ)) = getPointIndex(long, lat)
    if (nodeI != 0 && (cellI % gridSizePerCell) == 0 && nodeJ != 0 && (cellJ % gridSizePerCell) == 0)
      return true
    return false
  }

  def getOverlappingBorderPart(node: Int, pois: Iterable[POI]): (Int,(Array[Array[Int]], Array[Array[Int]], Array[Array[Int]])) = {
    val right: Array[Array[Int]] = Array.ofDim[Int](gridSizePerCell + 1, 2)
    val down: Array[Array[Int]] = Array.ofDim[Int](2, gridSizePerCell + 1)
    val corner: Array[Array[Int]] = Array.ofDim[Int](2, 2)
    for (poi <- pois.toList) {
      val (cellI,cellJ) =getCellIndexInGrid(node, poi.getPoint.getX, poi.getPoint.getY)
      if (cellI==gridSizePerCell||cellI==gridSizePerCell-1)
        right(cellJ)(cellI-gridSizePerCell+1)+=1
      if (cellJ==gridSizePerCell||cellJ==gridSizePerCell-1)
        down(cellJ-gridSizePerCell+1)(cellI)+=1
      if (((cellI==gridSizePerCell||cellI==gridSizePerCell-1))&&((cellJ==gridSizePerCell||cellJ==gridSizePerCell-1)))
        corner(cellJ-gridSizePerCell+1)(cellI-gridSizePerCell+1)+=1
    }
    return (node,(down, right, corner))
  }


  def getCellIndexInGrid(node: Int, long: Double, lat: Double): (Int, Int) = {
    val nodeI = (node - 1) % width
    var nodeJ = ((node - 1) / width.asInstanceOf[Double]).toInt
    var (cellI, cellJ) = getCellIndex(long, lat)
    return ((cellI - nodeI * gridSizePerCell), cellJ - nodeJ * gridSizePerCell)
  }
  def getCellIndexInGrid(node: Int,spatialObject: SpatialObject): (Int, Int) = {
    val nodeI = (node - 1) % width
    val nodeJ = ((node - 1) / width.asInstanceOf[Double]).toInt
    var a=(0,0)
    if(spatialObject.getGeometry.isInstanceOf[Point]) {
       a = getCellIndex(spatialObject.getGeometry.getCentroid.getX, spatialObject.getGeometry.getCentroid.getY)
    }
    else
      a = getCellIndex(spatialObject.getGeometry.getCoordinates.toList(1).x.toFloat
        , spatialObject.getGeometry.getCoordinates.toList(1).y.toFloat)
    return (a._1-nodeI*gridSizePerCell,a._2-nodeJ*gridSizePerCell)
  }

  def getNodeIndex(node:Int):(Int,Int)={
    val nodeI = (node - 1) % width
    val nodeJ = ((node - 1) / width.asInstanceOf[Double]).toInt
    return (nodeI,nodeJ)
  }

  def getLonLat(node:Int):(Double,Double)={
    val (nodeI,nodeJ)=getNodeIndex(node)
    return (minmaxLong._1 + nodeI*gridSizePerCell*cellSize , minmaxLat._2-nodeJ*gridSizePerCell*cellSize)
  }
}