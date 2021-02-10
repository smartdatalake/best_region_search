package SDL

import SDL.definitions.{Generic, GridIndexer}

import scala.collection.mutable
import scala.collection.mutable.{HashMap, ListBuffer, MultiMap, Set}

class DependencyGraph(gridIndexer: GridIndexer) {
  val indexToSpatialObj = new HashMap[(Int, Int), (SpatialObject, ListBuffer[SpatialObject])]
  // val indexToSpatialObjSafe = new HashMap[(Int, Int), Set[SpatialObject]] with MultiMap[(Int, Int), SpatialObject]
  // val indexToSpatialObjUnsafe = new HashMap[(Int, Int), Set[SpatialObject]] with MultiMap[(Int, Int), SpatialObject]
  val indexToSpatialObjM = new HashMap[(Int, Int), Set[SpatialObject]] with MultiMap[(Int, Int), SpatialObject]
  val indexToDependentNeighborCell = new mutable.HashSet[(Int, Int)]()
  val duplicate = new mutable.HashSet[String]
  var partition = -1
  var safeRegionCnt: Int = 0
  var cornerALong = -1
  var cornerALat = -1
  var cornerBLong = -1
  var cornerBLat = -1
  var minSafe = 1000000.0

  def setPartNum(a: Int) = {
    partition = a
    val (nodeI, nodeJ) = gridIndexer.getNodeIndex(a)
    cornerALong = nodeI * gridIndexer.gridSizePerCell
    cornerALat = nodeJ * gridIndexer.gridSizePerCell
    cornerBLong = cornerALong + gridIndexer.gridSizePerCell - 1
    cornerBLat = cornerALat + gridIndexer.gridSizePerCell - 1
  }

  def increaseSafeCNT() = {
    safeRegionCnt += 1
  }

  def addSafeRegion(spatialObject: SpatialObject): Unit = {
    val (cellI, cellJ) = gridIndexer.getCellIndex(spatialObject.getGeometry.getCoordinates.toList(1).x.toFloat
      , spatialObject.getGeometry.getCoordinates.toList(1).y.toFloat)
    val t = indexToSpatialObj.get((cellI, cellJ)).getOrElse(new SpatialObject, new ListBuffer[SpatialObject])
    indexToSpatialObj.+=(((cellI, cellJ), (spatialObject, t._2)))
    safeRegionCnt += 1
    if (minSafe > spatialObject.getScore)
      minSafe = spatialObject.getScore
  }

  def addUnsafeRegion(spatialObject: SpatialObject): Unit = {
    val (cellI, cellJ) = gridIndexer.getCellIndex(spatialObject.getGeometry.getCoordinates.toList(1).x.toFloat
      , spatialObject.getGeometry.getCoordinates.toList(1).y.toFloat)
    var t = indexToSpatialObj.get((cellI, cellJ)).getOrElse(null, null)
    if (t == (null, null)) {
      val p = new ListBuffer[SpatialObject];
      p += spatialObject
      indexToSpatialObj.+=(((cellI, cellJ), (null, p)))
    }
    else {
      t._2.+=(spatialObject)
      indexToSpatialObj.+=(((cellI, cellJ), (null, t._2)))
    }
  }

  def addM(spatialObject: SpatialObject): Unit = {
    indexToSpatialObjM.addBinding(gridIndexer.getCellIndex(spatialObject.getGeometry.getCoordinates.toList(1).x.toFloat
      , spatialObject.getGeometry.getCoordinates.toList(1).y.toFloat), spatialObject)
    if (minSafe > spatialObject.getScore)
      minSafe = spatialObject.getScore
    addDependentNeighboringCell(spatialObject)
  }

  def addDependentNeighboringCell(spatialObject: SpatialObject) = {
    val neighboringBorderCell = gridIndexer.getNeighborBorderCell(spatialObject.getGeometry.getCoordinates.toList(1).x.toFloat
      , spatialObject.getGeometry.getCoordinates.toList(1).y.toFloat)
    for (obj <- neighboringBorderCell)
      indexToDependentNeighborCell.add(obj)
  }

  def overlapCon(spatialObject: SpatialObject): Int = {
    val (cellI, cellJ) = gridIndexer.getCellIndex(spatialObject.getGeometry.getCoordinates.toList(1).x.toFloat
      , spatialObject.getGeometry.getCoordinates.toList(1).y.toFloat)
    var safe = false
    var unsafe = false
    for (i <- -1 to 1)
      for (j <- -1 to 1) {
        var t = indexToSpatialObj.get((cellI + i, cellJ + j)).getOrElse(null, null)
        if (t != (null, null)) {
          if (t != null && t._1 != null && Generic.intersects(spatialObject, t._1))
            safe = true
          if (t != null && t._2 != null && Generic.intersectsList(spatialObject, t._2))
            unsafe = true
        }
      }
    if (!safe && !unsafe)
      return 0
    if (safe)
      return 1
    if (unsafe)
      return 2
    return 0
  }


  def IsOverlapM(spatialObject: SpatialObject): Boolean = {
    val (cellI, cellJ) = gridIndexer.getCellIndex(spatialObject.getGeometry.getCoordinates.toList(1).x.toFloat
      , spatialObject.getGeometry.getCoordinates.toList(1).y.toFloat)
    for (i <- -1 to 1)
      for (j <- -1 to 1) {
        if (indexToSpatialObjM.contains((cellI + i, cellJ + j)))
          for (temp <- indexToSpatialObjM.get((cellI + i, cellJ + j)).get)
            if (Generic.intersects(spatialObject, temp))
              return true
      }
    return false;
  }

  def IsDependencyIncluded(spatialObject: SpatialObject): Boolean = {
    if (IsOverlapM(spatialObject))
      return true;
    if (IsBorderRegion(spatialObject)) {
      val neighboringBorderCell = gridIndexer.getNeighborBorderCell(spatialObject.getGeometry.getCoordinates.toList(1).x.toFloat
        , spatialObject.getGeometry.getCoordinates.toList(1).y.toFloat)
      for (obj <- neighboringBorderCell)
        if (indexToDependentNeighborCell.contains(obj))
          return true
    }
    return false;
  }

  def IsBorderRegion(spatialObject: SpatialObject): Boolean = {
    gridIndexer.IsOnBorderCell(spatialObject.getGeometry.getCoordinates.toList(1).x.toFloat
      , spatialObject.getGeometry.getCoordinates.toList(1).y.toFloat)
  }

  def getFinalResult(): List[SpatialObject] = {
    val t = new ListBuffer[SpatialObject]
    for (p <- indexToSpatialObj) {
      if (p._2._1 != null)
        t.+=(p._2._1)
      if (p._2._2 != null)
        for (x <- p._2._2)
          t.+=(x)
    }
    return t.toList
  }

  /*  override def toString(): String = {
    val safeCNT = indexToSpatialObjSafe.values.size;
    val UnsafeCNT = indexToSpatialObjUnsafe.values.size;
    val M_CNT = indexToSpatialObjM.values.size
    val dependentBorderCellCNT = indexToDependentNeighborCell.size
    return "Safe#" + safeCNT + ", Unsafe#" + UnsafeCNT + ", M_CNT#" + M_CNT + ", dependentBorderCell#" + dependentBorderCellCNT
  }*/

  def IsOnUpBorder(spatialObject: SpatialObject): Boolean = {
    gridIndexer.IsOnBorderUp(spatialObject.getGeometry.getCoordinates.toList(1).x.toFloat
      , spatialObject.getGeometry.getCoordinates.toList(1).y.toFloat)
  }

  def IsOnLeftBorder(spatialObject: SpatialObject): Boolean = {
    gridIndexer.IsOnBorderLeft(spatialObject.getGeometry.getCoordinates.toList(1).x.toFloat
      , spatialObject.getGeometry.getCoordinates.toList(1).y.toFloat)
  }

  def IsOnCornerBorder(spatialObject: SpatialObject): Boolean = {
    gridIndexer.IsOnBorderCorner(spatialObject.getGeometry.getCoordinates.toList(1).x.toFloat
      , spatialObject.getGeometry.getCoordinates.toList(1).y.toFloat)
  }

  def SetMinSafe(a: Int) = {
    minSafe = a
  }
}

/*class DependencyGraph (gridIndexer: GridIndexer) {
 // val indexToSpatialObj = new HashMap[(Int, Int), (SpatialObject,SpatialObject)] with MultiMap[(Int, Int), SpatialObject]
  val indexToSpatialObjSafe = new HashMap[(Int, Int), Set[SpatialObject]] with MultiMap[(Int, Int), SpatialObject]
  val indexToSpatialObjUnsafe = new HashMap[(Int, Int), Set[SpatialObject]] with MultiMap[(Int, Int), SpatialObject]
  val indexToSpatialObjM = new HashMap[(Int, Int), Set[SpatialObject]] with MultiMap[(Int, Int), SpatialObject]
  val indexToDependentNeighborCell = new mutable.HashSet[(Int, Int)]()
  var safeRegionCnt: Int = 0

  def increaseSafeCNT() = {
    safeRegionCnt += 1
  }

  def addSafeRegion(spatialObject: SpatialObject): Unit = {
    indexToSpatialObjSafe.addBinding(gridIndexer.getCellIndex(spatialObject.getGeometry.getCoordinates.toList(1).x.toFloat
      , spatialObject.getGeometry.getCoordinates.toList(1).y.toFloat), spatialObject)
    safeRegionCnt += 1
  }

  def addUnsafeRegion(spatialObject: SpatialObject): Unit = {
    indexToSpatialObjUnsafe.addBinding(gridIndexer.getCellIndex(spatialObject.getGeometry.getCoordinates.toList(1).x.toFloat
      , spatialObject.getGeometry.getCoordinates.toList(1).y.toFloat), spatialObject)
  }

  def addM(spatialObject: SpatialObject): Unit = {
    indexToSpatialObjM.addBinding(gridIndexer.getCellIndex(spatialObject.getGeometry.getCoordinates.toList(1).x.toFloat
      , spatialObject.getGeometry.getCoordinates.toList(1).y.toFloat), spatialObject)
    addDependentNeighboringCell(spatialObject)
  }

  def addDependentNeighboringCell(spatialObject: SpatialObject) = {
    val neighboringBorderCell = gridIndexer.getNeighborBorderCell(spatialObject.getGeometry.getCoordinates.toList(1).x.toFloat
      , spatialObject.getGeometry.getCoordinates.toList(1).y.toFloat)
    for (obj <- neighboringBorderCell)
      indexToDependentNeighborCell.add(obj)
  }

  def IsOverlapAnyRegion(spatialObject: SpatialObject): Boolean = {
    val (cellI, cellJ) = gridIndexer.getCellIndex(spatialObject.getGeometry.getCoordinates.toList(1).x.toFloat
      , spatialObject.getGeometry.getCoordinates.toList(1).y.toFloat)
    for (i <- -1 to 1)
      for (j <- -1 to 1) {
        if (indexToSpatialObjSafe.contains((cellI + i, cellJ + j)))
          for (temp <- indexToSpatialObjSafe.get((cellI + i, cellJ + j)).get)
            if (Generic.intersects(spatialObject, temp))
              return true
        if (indexToSpatialObjUnsafe.contains((cellI + i, cellJ + j)))
          for (temp <- indexToSpatialObjUnsafe.get((cellI + i, cellJ + j)).get)
            if (Generic.intersects(spatialObject, temp))
              return true
      }
    return false;
  }

  def IsOverlapSafeRegion(spatialObject: SpatialObject): Boolean = {
    val (cellI, cellJ) = gridIndexer.getCellIndex(spatialObject.getGeometry.getCoordinates.toList(1).x.toFloat
      , spatialObject.getGeometry.getCoordinates.toList(1).y.toFloat)
    for (i <- -1 to 1)
      for (j <- -1 to 1)
        if (indexToSpatialObjSafe.contains((cellI + i, cellJ + j)))
          for (temp <- indexToSpatialObjSafe.get((cellI + i, cellJ + j)).get)
            if (Generic.intersects(spatialObject, temp))
              return true
    return false;
  }

  def IsOverlapUnsafeRegion(spatialObject: SpatialObject): Boolean = {
    val (cellI, cellJ) = gridIndexer.getCellIndex(spatialObject.getGeometry.getCoordinates.toList(1).x.toFloat
      , spatialObject.getGeometry.getCoordinates.toList(1).y.toFloat)
    for (i <- -1 to 1)
      for (j <- -1 to 1)
        if (indexToSpatialObjUnsafe.contains((cellI + i, cellJ + j)))
          for (temp <- indexToSpatialObjUnsafe.get((cellI + i, cellJ + j)).get)
            if (Generic.intersects(spatialObject, temp))
              return true
    return false;
  }

  def IsOverlapM(spatialObject: SpatialObject): Boolean = {
    val (cellI, cellJ) = gridIndexer.getCellIndex(spatialObject.getGeometry.getCoordinates.toList(1).x.toFloat
      , spatialObject.getGeometry.getCoordinates.toList(1).y.toFloat)
    for (i <- -1 to 1)
      for (j <- -1 to 1) {
        if (indexToSpatialObjM.contains((cellI + i, cellJ + j)))
          for (temp <- indexToSpatialObjM.get((cellI + i, cellJ + j)).get)
            if (Generic.intersects(spatialObject, temp))
              return true
      }
    return false;
  }

  def IsDependencyIncluded(spatialObject: SpatialObject): Boolean = {
    if (IsOverlapM(spatialObject))
      return true;
    if (IsBorderRegion(spatialObject)) {
      val neighboringBorderCell = gridIndexer.getNeighborBorderCell(spatialObject.getGeometry.getCoordinates.toList(1).x.toFloat
        , spatialObject.getGeometry.getCoordinates.toList(1).y.toFloat)
      for (obj <- neighboringBorderCell)
        if (indexToDependentNeighborCell.contains(obj))
          return true
    }
    return false;
  }

  def IsBorderRegion(spatialObject: SpatialObject): Boolean = {
    gridIndexer.IsOnBorderCell(spatialObject.getGeometry.getCoordinates.toList(1).x.toFloat
      , spatialObject.getGeometry.getCoordinates.toList(1).y.toFloat)
  }

  def getFinalResult(): List[SpatialObject] = {
    return indexToSpatialObjSafe.values.flatten.toList ::: indexToSpatialObjUnsafe.values.flatten.toList
  }

  override def toString(): String = {
    val safeCNT = indexToSpatialObjSafe.values.size;
    val UnsafeCNT = indexToSpatialObjUnsafe.values.size;
    val M_CNT = indexToSpatialObjM.values.size
    val dependentBorderCellCNT = indexToDependentNeighborCell.size
    return "Safe#" + safeCNT + ", Unsafe#" + UnsafeCNT + ", M_CNT#" + M_CNT + ", dependentBorderCell#" + dependentBorderCellCNT
  }

  def IsOnUpBorder(spatialObject: SpatialObject): Boolean = {
    gridIndexer.IsOnBorderUp(spatialObject.getGeometry.getCoordinates.toList(1).x.toFloat
      , spatialObject.getGeometry.getCoordinates.toList(1).y.toFloat)
  }

  def IsOnLeftBorder(spatialObject: SpatialObject): Boolean = {
    gridIndexer.IsOnBorderLeft(spatialObject.getGeometry.getCoordinates.toList(1).x.toFloat
      , spatialObject.getGeometry.getCoordinates.toList(1).y.toFloat)
  }

  def IsOnCornerBorder(spatialObject: SpatialObject): Boolean = {
    gridIndexer.IsOnBorderCorner(spatialObject.getGeometry.getCoordinates.toList(1).x.toFloat
      , spatialObject.getGeometry.getCoordinates.toList(1).y.toFloat)
  }
}*/



