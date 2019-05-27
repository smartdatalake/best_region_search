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


  def poiToKeyValue(row: Row, width: Int, minmaxLong: (Double, Double), minmaxLat: (Double, Double), eps: Double
                    , geometryFactory: GeometryFactory, gridIndexer: GridIndexer): Seq[(Int, POI)] = {
    var gridSize = 0.0
    if ((minmaxLong._2 - minmaxLong._1) > (minmaxLat._2 - minmaxLat._1))
      gridSize = (minmaxLong._2 - minmaxLong._1) / width
    else
      gridSize = (minmaxLat._2 - minmaxLat._1) / width
    val cellSize = gridSize * eps

    val keywords = row.getAs[String]("keywords").split(",").toList;
    val long = row.getAs[Double]("longtitude")
    val lat = row.getAs[Double]("latitude")
    //val newNode = extractNode(x.get(0), x.get(1), width, minmaxLong, minmaxLat);
    val result = ListBuffer[(Int, POI)]()
   // result.add((1,new POI(row.getAs[String]("id"), row.getAs[String]("name")
   //   , row.getAs[Double]("longtitude"), row.getAs[Double]("latitude"), keywords, 0, geometryFactory)))
   // return result
    return gridIndexer.getNodeIndex(row.getAs[Double]("longtitude"), row.getAs[Double]("latitude"))
      .map(x => (x._2 * width + x._1 + 1, new POI(row.getAs[String]("id"), row.getAs[String]("name")
        , row.getAs[Double]("longtitude"), row.getAs[Double]("latitude"), keywords, 0, geometryFactory)))
  }

  def intersects(point1: SpatialObject, point2: SpatialObject): Boolean = {
    point1.getGeometry().intersects(point2.getGeometry())
  }

  def intersectsList(point: SpatialObject, list: ListBuffer[SpatialObject]): Boolean = {
    for (point2 <- list) {
      if (intersects(point, point2)) {
        true
      }
    }
    false
  }
}
/*    for(node <-1 until (width*width)+1 ){
      if(IsInNodeRegion(long,lat,node,gridSize.asInstanceOf[Double],cellSize.asInstanceOf[Double],minmaxLong._1,minmaxLat._2,width)){

      }
     //   result.add((node, new POI(x.getAs[String]("id"), x.getAs[String]("name"), x.getAs[Double]("longtitude"), x.getAs[Double]("latitude"), keywords, 0, geometryFactory)))
   }
    //
   /* val regionI=((long - minmaxLong._1) / gridSize).toInt
    val regionJ=((lat - minmaxLat._1) / gridSize).toInt
    result.add((width*regionJ + regionI, new POI(x.getAs[String]("id"), x.getAs[String]("name"), x.getAs[Double]("longtitude"), x.getAs[Double]("latitude"), keywords, 0, geometryFactory)))
    if(long-(minmaxLong._1+regionI*gridSize)<cellSize&&(minmaxLat._1+(regionJ+1)*gridSize)-lat<cellSize&&regionI>0&&regionJ<width-1){
      result.add((width*regionJ + (regionI-1), new POI(x.getAs[String]("id"), x.getAs[String]("name"), x.getAs[Double]("longtitude"), x.getAs[Double]("latitude"), keywords, 0, geometryFactory)))
      result.add((width*(regionJ+1) + regionI, new POI(x.getAs[String]("id"), x.getAs[String]("name"), x.getAs[Double]("longtitude"), x.getAs[Double]("latitude"), keywords, 0, geometryFactory)))
      result.add((width*(regionJ+1) + (regionI-1), new POI(x.getAs[String]("id"), x.getAs[String]("name"), x.getAs[Double]("longtitude"), x.getAs[Double]("latitude"), keywords, 0, geometryFactory)))
    }
    else if(long-(minmaxLong._1+regionI*gridSize)<cellSize&&regionI>0){
      result.add((width*regionJ + (regionI-1), new POI(x.getAs[String]("id"), x.getAs[String]("name"), x.getAs[Double]("longtitude"), x.getAs[Double]("latitude"), keywords, 0, geometryFactory)))
    }
    else if((minmaxLat._1+(regionJ+1)*gridSize)-lat<cellSize&&regionJ<width-1){
      result.add((width*(regionJ+1) + regionI, new POI(x.getAs[String]("id"), x.getAs[String]("name"), x.getAs[Double]("longtitude"), x.getAs[Double]("latitude"), keywords, 0, geometryFactory)))
    }*/

    result*/