package matt.definitions

import org.apache.spark.sql.Row
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import org.locationtech.jts.geom.GeometryFactory;
import matt.POI;
import matt.SpatialObject;

object Generic {
  def IsInNodeRegion(long: Double, lat: Double, node: Int, gridSize: Double, cellSize: Double, minLong: Double
                     , maxLat: Double, width: Int): Boolean = {
    val nodeJ = ((node - 1) / width).toInt
    val nodeI = node - nodeJ * width - 1
    if (nodeI * gridSize + minLong <= long && long < (nodeI + 1) * gridSize + minLong + cellSize)
      if (maxLat - (nodeJ + 1) * gridSize - cellSize <= lat && lat < maxLat - nodeJ * gridSize)
        return true
    return false
  }

  def borderPOIToKeyValue(row: Row, geometryFactory: GeometryFactory, gridIndexer: GridIndexer): Seq[((Int, Boolean), POI)] = {
    val pos = gridIndexer.getNodeNumber_Pos_Border(row.getAs[Double]("longtitude"), row.getAs[Double]("latitude"))
    val keywords = row.getAs[String]("keywords").split(",").toList;
    return pos.map(x => (x, new POI(row.getAs[String]("id"), row.getAs[String]("name")
      , row.getAs[Double]("longtitude"), row.getAs[Double]("latitude"), keywords, 0, geometryFactory)))
  }

  def poiToKeyValue(row: Row, geometryFactory: GeometryFactory, gridIndexer: GridIndexer): Seq[(Int, POI)] = {
    val keywords = row.getAs[String]("keywords").split(",").toList;
    return gridIndexer.getNodeIndex(row.getAs[Double]("longtitude"), row.getAs[Double]("latitude"))
      .map(x => (x._2 * gridIndexer.width + x._1 + 1, new POI(row.getAs[String]("id"), row.getAs[String]("name")
        , row.getAs[Double]("longtitude"), row.getAs[Double]("latitude"), keywords, 0, geometryFactory)))
  }

  def intersects(point1: SpatialObject, point2: SpatialObject): Boolean = {
    point1.getGeometry().intersects(point2.getGeometry())
  }

  def intersectsList(point: SpatialObject, list: ListBuffer[SpatialObject]): Boolean = {
    for (point2 <- list)
      if (intersects(point, point2))
        return true
    return false
  }



}