package matt

import matt.definitions.{Generic, GridIndexer}

import scala.collection.mutable



class DependencyGraph (nodeNum:Int,gridIndexer: GridIndexer) {
  val indexToSpatialObjIdx: mutable.HashMap[(Int, Int), SpatialObjectIdx] = new mutable.HashMap()
  val safeRegionCnt: Int = 0

  def add(spatialObject: SpatialObject):Unit = {
    val newNode:SpatialObjectIdx = new SpatialObjectIdx(spatialObject)
    addDependencyOverlapping(newNode)
    indexToSpatialObjIdx+=(gridIndexer.getCellIndex(newNode.leftUpperPOI._1, newNode.leftUpperPOI._2) -> newNode)
  }

  def add(spatialObjects: Iterable[SpatialObject]) = {
    for (spatialObject <- spatialObjects) {
      val newNode:SpatialObjectIdx = new SpatialObjectIdx(spatialObject)
      addDependencyOverlapping(newNode)
      indexToSpatialObjIdx+=(gridIndexer.getCellIndex(newNode.leftUpperPOI._1, newNode.leftUpperPOI._2) -> newNode)
    }
  }

  def addDependencyOverlapping(spatialObjectIdx: SpatialObjectIdx)= {
    val (cellI, cellJ) = gridIndexer.getCellIndex(spatialObjectIdx.leftUpperPOI._1, spatialObjectIdx.leftUpperPOI._2)
    for (i <- -1 to 1) {
      for (j <- -1 to 1) {
        if (indexToSpatialObjIdx.contains((cellI + i, cellJ + j))) {
          val cantidateNode = indexToSpatialObjIdx.get((cellI + i, cellJ + j)).get
          if (Generic.intersects(spatialObjectIdx.region, cantidateNode.region))
            if (spatialObjectIdx.region.compareTo(cantidateNode.region) <= 0)
              spatialObjectIdx.addDependencyOverlapping(cantidateNode)
            else
              cantidateNode.addDependencyOverlapping(spatialObjectIdx)
        }
      }
    }
  }


}