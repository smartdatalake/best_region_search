package matt.definitions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.{ Encoder, Encoders }
import org.apache.spark.sql.functions.{ when, lower, min, max }
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;
//import java.util.List;
import java.util.Properties;

import scala.util.control.Breaks._
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;

import matt.POI;
import matt.SpatialObject;
import matt.Grid;
import matt.ca.BCAFinder;
import matt.ca.BCAIndexProgressive;
import matt.ca.BCAIndexProgressiveDiv;
import matt.ca.BCAIndexProgressiveDivExhaustive;
import matt.ca.UtilityScoreFunction;
import matt.io.InputFileParser;
import matt.io.ResultsWriter;
import matt.score.ScoreFunction;
import matt.score.ScoreFunctionCount;

object Generic {
  def extractNode(long: Any, lat: Any, nodes: Int, minmaxLong: (Any, Any), minmaxLat: (Any, Any)): Int = {
    5;
  }

  def rowToPOI(thisRow: Row, geometryFactory: GeometryFactory): POI = {

    val keywords = thisRow.getAs[String]("keywords").split(",").toList;
    //    print("keyword:\t")
    //    for (keyword <- keywords) {
    //      print(keyword + "\t");
    //    }
    //    println(thisRow);

    val newPOI = new POI(thisRow.getAs[String]("id"), thisRow.getAs[String]("name"), thisRow.getAs[Float]("longtitude"), thisRow.getAs[Float]("latitude"), keywords, 0, geometryFactory);

    //    println("newPOI:\t" + newPOI.getId() + "\t" + newPOI.getName());

    newPOI;
  }

  def poiToKeyValue(x: Row, width: Int, minmaxLong: (Any, Any), minmaxLat: (Any, Any), geometryFactory: GeometryFactory): (Int, POI) = {
    val newPOI = rowToPOI(x, geometryFactory: GeometryFactory);
    val newNode = extractNode(x.get(0), x.get(1), width, minmaxLong, minmaxLat);

    //    println("newPOI:\t" + newPOI.toString());

    (newNode, newPOI)
  }

  def intersects(point1: SpatialObject, point2: SpatialObject): Boolean = {
    point1.getGeometry().intersects(point2.getGeometry())
  }

  def intersectsList(point: SpatialObject, list: ListBuffer[POI]): Boolean = {
    for (point2 <- list) {
      if (intersects(point, point2)) {
        true
      }
    }
    false
  }
}