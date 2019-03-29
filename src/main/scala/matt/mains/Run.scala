package matt.mains

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

import matt.definitions.TableDefs
import matt.definitions.Generic

import matt.distrib.NstepAlgo

object Run {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .appName("Simple Application")
      .getOrCreate()

    import spark.implicits;

    //		/* load configuration file */
    //		val prop = new Properties();
    //		prop.load(new FileInputStream("config.properties"));
    //
    //		val eps = prop.getProperty("ca-eps").toDouble;
    //		val topk = prop.getProperty("ca-topk").toInt;
    //		val distinct = prop.getProperty("ca-distinct").toBoolean;
    //		val div = prop.getProperty("ca-div").toBoolean;
    //		val exhaustive = prop.getProperty("ca-exhaustive").toBoolean
    //		val decayConstant = prop.getProperty("ca-decay-constant").toDouble;
    //		val printResults = true;

    val poiInputFile = "/cloud_store/olma/spark/input/osmpois-europe.csv";

    val eps = 0.001
    // choose number of expected results
    val topk = 10
    val decayConstant = 0.5

    val inputData = spark.read.format("csv").option("header", "true").option("delimiter", ";").schema(TableDefs.customSchema2).load(poiInputFile);
    inputData.show(false);

    // set number of cores
    val cores = 24
    val width = scala.math.sqrt(cores).toInt;

    // choose width of grid (the number of cores required is N squared)
    val minmaxLongArray = inputData.agg(min("longtitude"), max("longtitude")).rdd.map(r => r(0)).collect()
    val minmaxLong = (minmaxLongArray.head, minmaxLongArray.last);
    //    println("\n\nminmaxLONG: " + minmaxLong + "\n\n");
    val minmaxLatArray = inputData.agg(min("latitude"), max("latitude")).rdd.map(r => r(0)).collect()
    val minmaxLat = (minmaxLatArray.head, minmaxLatArray.last);
    //    println("\n\nminmaxLat: " + minmaxLat + "\n\n");

    // find to which node does each point belongs to : (NodeNo,Row)
    val geometryFactory = new GeometryFactory(new PrecisionModel(), 4326);
    val nodeToPoint = inputData.rdd.map(x => Generic.poiToKeyValue(x, width, minmaxLong, minmaxLat, geometryFactory));
    //    val temp = nodeToPoint.collect();

    val Nstep = true;
    val OneStep = false;

    if (Nstep) {
      matt.distrib.NstepAlgo.Run(nodeToPoint, eps, decayConstant, topk);
    }

    if (OneStep) {
      matt.distrib.OnestepAlgo.Run(nodeToPoint, eps, decayConstant, topk);
    }

    spark.stop()
  }

}