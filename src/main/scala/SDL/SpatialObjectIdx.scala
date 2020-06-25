package SDL

import scala.collection.mutable.ListBuffer

class SpatialObjectIdx (spatialObject: SpatialObject){

  val leftUpperPOI:(Double,Double) = (spatialObject.getGeometry.getCoordinates.toList(1).x.toFloat,spatialObject.getGeometry.getCoordinates.toList(1).y.toFloat)
  var region:SpatialObject=spatialObject
  var dependenciesOverlapping: ListBuffer[SpatialObjectIdx] = ListBuffer[SpatialObjectIdx]();
  def addDependencyOverlapping(spatialObjectIdx: SpatialObjectIdx)={
    dependenciesOverlapping+=spatialObjectIdx
  }
}