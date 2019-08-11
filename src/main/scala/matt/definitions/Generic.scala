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
    return pos.map(x => (x, new POI(row.getAs[String]("id")
      , row.getAs[Double]("longtitude"), row.getAs[Double]("latitude"), 1, geometryFactory)))
  }

  def poiToKeyValue(row: Row, geometryFactory: GeometryFactory, gridIndexer: GridIndexer): Seq[(Int, POI)] = {
    val keywords = row.getAs[String]("keywords").split(",").toList;
    return gridIndexer.getNodeIndex(row.getAs[Double]("longtitude"), row.getAs[Double]("latitude"))
      .map(x => (x._2 * gridIndexer.width + x._1 + 1, new POI(row.getAs[String]("id")
        , row.getAs[Double]("longtitude"), row.getAs[Double]("latitude"), 1, geometryFactory)))
  }


  def poiToKeyValueShifting(row: Row, geometryFactory: GeometryFactory, gridIndexer: GridIndexer, shift: Double): Seq[(Int, POI)] = {
    val keywords = row.getAs[String]("keywords").split(",").toList;
    val result = new ListBuffer[(Int, POI)]()
    result.addAll(gridIndexer.getNodeIndex(row.getAs[Double]("longtitude"), row.getAs[Double]("latitude"))
      .map(x => (x._2 * gridIndexer.width + x._1 + 1, new POI(row.getAs[String]("id")
        , row.getAs[Double]("longtitude"), row.getAs[Double]("latitude"), 1, geometryFactory))))

    result.addAll(gridIndexer.getNodeIndex(row.getAs[Double]("longtitude") + shift, row.getAs[Double]("latitude"))
      .map(x => (x._2 * gridIndexer.width + x._1 + 1, new POI(row.getAs[String]("id")
        , row.getAs[Double]("longtitude") + shift, row.getAs[Double]("latitude"), 1, geometryFactory))))

    result.addAll(gridIndexer.getNodeIndex(row.getAs[Double]("longtitude"), row.getAs[Double]("latitude") + shift)
      .map(x => (x._2 * gridIndexer.width + x._1 + 1, new POI(row.getAs[String]("id")
        , row.getAs[Double]("longtitude"), row.getAs[Double]("latitude") + shift, 1, geometryFactory))))

    result.addAll(gridIndexer.getNodeIndex(row.getAs[Double]("longtitude") + shift, row.getAs[Double]("latitude") + shift)
      .map(x => (x._2 * gridIndexer.width + x._1 + 1, new POI(row.getAs[String]("id")
        , row.getAs[Double]("longtitude") + shift, row.getAs[Double]("latitude") + shift, 1, geometryFactory))))
    return result.toList
  }

  def intersects(point1: SpatialObject, point2: SpatialObject): Boolean = {
    if (point2 == null) return false
    if (point1 == null) return false
    point1.getGeometry().intersects(point2.getGeometry())
  }

  def intersectsList(point: SpatialObject, list: ListBuffer[SpatialObject]): Boolean = {
    if (list.size == 0) return false
    for (point2 <- list)
      if (intersects(point, point2))
        return true
    return false
  }

  def poiOptToKeyValue(row: Row, geometryFactory: GeometryFactory, gridIndexer: GridIndexer): Seq[(Int, POI)] = {
    val keywords = row.getAs[String]("keywords").split(",").toList;
    return gridIndexer.getNodeOptIndex(row.getAs[Double]("longtitude"), row.getAs[Double]("latitude"))
      .map(x => (x._2 * gridIndexer.width + x._1 + 1, new POI(row.getAs[String]("id")
        , row.getAs[Double]("longtitude"), row.getAs[Double]("latitude"), 1, geometryFactory)))
  }


}