package matt

import matt.definitions.{Generic, GridIndexer}

import scala.collection.mutable

import collection.mutable.{ HashMap, MultiMap, Set }

class DependencyGraph (gridIndexer: GridIndexer) {
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

  override def toString():String={
    val safeCNT=indexToSpatialObjSafe.values.size;
    val UnsafeCNT=indexToSpatialObjUnsafe.values.size;
    val M_CNT=indexToSpatialObjM.values.size
    val dependentBorderCellCNT=indexToDependentNeighborCell.size
    return "Safe#"+safeCNT+", Unsafe#"+UnsafeCNT+", M_CNT#"+ M_CNT+", dependentBorderCell#"+dependentBorderCellCNT
  }
}



