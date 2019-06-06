package matt.mains

import java.net.URI
import java.util

import matt.POI
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{lower, max, min, when}
import matt.definitions.GridIndexer
import org.apache.spark.{SparkConf, SparkContext}
import org.locationtech.jts.geom.{GeometryFactory, PrecisionModel}
import matt.definitions.TableDefs
import matt.definitions.Generic
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.collection.mutable

object Run {
 def main(args: Array[String]) {

  ///////Param & Config
  //////////////////////////
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val spark = SparkSession
    .builder
   // .master("local[*]")
    .appName("Simple Application")
    .config("spark.executor.memory", "7g")
    .config("spark.driver.maxResultSize", "3g")
    .config("spark.driver.memory", "7g")
    .config("spark.memory.offHeap.enabled", true)
    .config("spark.memory.offHeap.size", "7g")
    .getOrCreate()
  val hadoopConfig: Configuration = spark.sparkContext.hadoopConfiguration
  hadoopConfig.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
  hadoopConfig.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
  val poiInputFile = "/home/hamid/5.csv";
  val poiInputFile2 = "/home/hamid/temp.csv";
  val poiInputFile3 = "/home/hamid/input.csv";

  val eps = 0.001
  // choose number of expected results
  val topk = 300
  val decayConstant = 0.5
  val cores = 128*128

  val width = scala.math.sqrt(cores).toInt
  //////end Param & config
  //////////////////////////////////////

  //////Read and split CSV coordination to (nodeNumber, POI) (assign poi to each worker)
  ///////////////////////////////////////////////////////////////

  val inputData = spark.read.format("csv").option("header", "true").option("delimiter", ";").schema(TableDefs.customSchema2).load("hdfs:///input.csv").drop().filter(x => (x.getAs[Double]("longtitude") != null && x.getAs[Double]("latitude") != null));;
  //val inputData = spark.read.format("csv").option("header", "true").option("delimiter", ";").schema(TableDefs.customSchema2).load(poiInputFile).drop().filter(x => (x.getAs[Double]("longtitude") != null && x.getAs[Double]("latitude") != null));

  val minLong = inputData.select("longtitude").reduce((x, y) => if (x.getAs[Double]("longtitude") < y.getAs[Double]("longtitude")) x else y).getAs[Double](0)
  val maxLong = inputData.select("longtitude").reduce((x, y) => if (x.getAs[Double]("longtitude") > y.getAs[Double]("longtitude")) x else y).getAs[Double](0)
  val minLat = inputData.select("latitude").reduce((x, y) => if (x.getAs[Double]("latitude") < y.getAs[Double]("latitude")) x else y).getAs[Double](0)
  val maxLat = inputData.select("latitude").reduce((x, y) => if (x.getAs[Double]("latitude") > y.getAs[Double]("latitude")) x else y).getAs[Double](0)

  val minmaxLong = (minLong - 0.01, maxLong + 0.01);
  println("\n\nminmaxLONG: " + minmaxLong + "\n\n");
  val minmaxLat = (minLat - 0.01, maxLat + 0.01);
  println("\n\nminmaxLat: " + minmaxLat + "\n\n");

  val gridIndexer = new GridIndexer(width, eps, minmaxLong, minmaxLat)
  println(gridIndexer.gridSizePerCell)
  // find to which node does each point belongs to : (NodeNo,Row)
  val geometryFactory = new GeometryFactory(new PrecisionModel(), 4326);
  val nodeToPoint = inputData.rdd.flatMap(x => Generic.poiToKeyValue(x, geometryFactory, gridIndexer)).cache();
  val p=nodeToPoint.groupByKey().map(x=>{
   gridIndexer.getOverlappingBorderPart(x._1,x._2)
  })
  val oo:mutable.HashMap[Int,(Array[Array[Int]],Array[Array[Int]],Array[Array[Int]])]=new mutable.HashMap[Int,(Array[Array[Int]],Array[Array[Int]],Array[Array[Int]])]()
  p.foreach(x=>oo.put(x._1,x._2))
  val borderPOI = inputData.rdd.map(row => {
   (gridIndexer.getNodeNumber_Border(row.getAs[Double]("longtitude")
    , row.getAs[Double]("latitude")), new POI(row.getAs[String]("id"), row.getAs[String]("name")
    , row.getAs[Double]("longtitude"), row.getAs[Double]("latitude"), new util.ArrayList[String](), 0, geometryFactory))
  }).filter(x => x._1 > 0)
  //println(borderPOI.collect().toList.size)
  ////////End Read & split data poi to each worker
  //////////////////////////////////////////////////////////////////////////////


  ///////Start
  //////////////////////////////
  val Nstep = false;
  val Nstep2 = false
  val OneStep = false;
  val OneStepOptimized = true

  if (Nstep) {
   matt.distrib.NstepAlgo.Run(nodeToPoint, eps, topk);
  }

  if (Nstep2) {
   matt.distrib.NstepAlgo2.Run(nodeToPoint, eps, topk, width)
  }

  if (OneStep) {
   matt.distrib.OnestepAlgo.Run(nodeToPoint, eps, decayConstant, topk, gridIndexer);
  }
 if (OneStepOptimized) {
  println("opt")
   matt.distrib.OnestepAlgoOptimized.Run(nodeToPoint, oo, eps, decayConstant, topk, gridIndexer)
  }

  spark.stop()
 };

}