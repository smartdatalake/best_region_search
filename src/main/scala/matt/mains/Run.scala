package matt.mains

import org.apache.spark.sql.SparkSession
import matt.definitions.GridIndexer
import org.locationtech.jts.geom.{GeometryFactory, PrecisionModel}
import matt.definitions.TableDefs
import matt.definitions.Generic
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.Logger
import org.apache.log4j.Level

object Run {
 def main(args: Array[String]) {

  ///////Param & Config
  //////////////////////////
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("Simple Application")
    .config("spark.executor.memory", "7g")
    .config("spark.driver.memory", "7g")
    .config("spark.network.timeout", "60000s")
    .config("spark.executor.heartbeatInterval", "10000s")
    .config("spark.shuffle.blockTransferService", "nio")
    .getOrCreate()
  val hadoopConfig: Configuration = spark.sparkContext.hadoopConfiguration
  hadoopConfig.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
  hadoopConfig.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
  val poiInputFile = "/home/hamid/osmpois-planet-cleaned.csv";
  val poiInputFile65 = "/home/hamid/5" + ".csv";
  val poiInputFile2 = "/home/hamid/temp.csv";
  val poiInputFile3 = "/home/hamid/input.csv";

  val eps = 0.001
  val topk = 20
  val decayConstant = 0.5
  val cores = 4 * 4

  print("gfgfg")
  val width = scala.math.sqrt(cores).toInt
  //////end Param & config
  //////////////////////////////////////

  //////Read and split CSV coordination to (nodeNumber, POI) (assign poi to each worker)
  ///////////////////////////////////////////////////////////////

  //val inputData = spark.read.format("csv").option("header", "true").option("delimiter", ";").schema(TableDefs.customSchema2).load("hdfs:///input2.csv").drop().filter(x => (x.getAs[Double]("longtitude") != null && x.getAs[Double]("latitude") != null)).filter(x => (x.getAs[Double]("longtitude")> -0.489 && x.getAs[Double]("longtitude")< 0.236)).filter(x => (x.getAs[Double]("latitude")> 51.28 && x.getAs[Double]("latitude")< 51.686));
  var inputData = spark.read.format("csv").option("header", "true").option("delimiter", ";").schema(TableDefs.customSchema2).load(poiInputFile).drop().filter(x => (x.getAs[Double]("longtitude") != null && x.getAs[Double]("latitude") != null)).filter(x => (x.getAs[Double]("longtitude") > -0.489 && x.getAs[Double]("longtitude") < 0.236)).filter(x => (x.getAs[Double]("latitude") > 51.28 && x.getAs[Double]("latitude") < 51.686));

  val minLong = inputData.select("longtitude").reduce((x, y) => if (x.getAs[Double]("longtitude") < y.getAs[Double]("longtitude")) x else y).getAs[Double](0)
  val maxLong = inputData.select("longtitude").reduce((x, y) => if (x.getAs[Double]("longtitude") > y.getAs[Double]("longtitude")) x else y).getAs[Double](0)
  val minLat = inputData.select("latitude").reduce((x, y) => if (x.getAs[Double]("latitude") < y.getAs[Double]("latitude")) x else y).getAs[Double](0)
  val maxLat = inputData.select("latitude").reduce((x, y) => if (x.getAs[Double]("latitude") > y.getAs[Double]("latitude")) x else y).getAs[Double](0)

  val minmaxLong = (minLong - 0.0001, maxLong + 0.0001);
  println("\n\nminmaxLONG: " + minmaxLong + "\n\n");
  val minmaxLat = (minLat - 0.0001, maxLat + 0.0001);
  println("\n\nminmaxLat: " + minmaxLat + "\n\n");
  println(inputData.collect().size)
  val gridIndexer = new GridIndexer(width, eps, minmaxLong, minmaxLat)
  println(gridIndexer.gridSizePerCell)
  // find to which node does each point belongs to : (NodeNo,Row)
  val geometryFactory = new GeometryFactory(new PrecisionModel(), 4326);
  val nodeToPoint = inputData.rdd.flatMap(x => Generic.poiToKeyValue(x, geometryFactory, gridIndexer));
  ////////End Read & split data poi to each worker
  //////////////////////////////////////////////////////////////////////////////


  ///////Start
  //////////////////////////////
  val Nstep = true;
  val Nstep2 = false
  val OneStep = true;
  val OneStepOptimized = true

  if (Nstep) {
   val t = System.nanoTime()
   matt.distrib.NstepAlgo.Run(nodeToPoint, eps, topk);
   println("Nstep:::       time:" + (System.nanoTime() - t) + "          eps:" + eps + "       topk:" + topk + "     cores:" + cores)
  }

  /* if (Nstep2) {
   matt.distrib.NstepAlgo2.Run(nodeToPoint, eps, topk, width)
  }*/

  if (OneStep) {
   val t = System.nanoTime()
   matt.distrib.OnestepAlgo.Run(nodeToPoint, eps, topk, gridIndexer);
   println("Single:::       time:" + (System.nanoTime() - t) + "          eps:" + eps + "       topk:" + topk + "     cores:" + cores)
  }
  if (OneStepOptimized) {
   val t = System.nanoTime()
   val nodeOptToPoint = inputData.rdd.flatMap(x => Generic.poiOptToKeyValue(x, geometryFactory, gridIndexer));
   matt.distrib.OnestepAlgoOptimized.Run(nodeOptToPoint, eps, decayConstant, topk, gridIndexer)
   println("SingleOpt:::       time:" + (System.nanoTime() - t) + "          eps:" + eps + "       topk:" + topk + "     cores:" + cores)
  }
  spark.stop()
 };

}