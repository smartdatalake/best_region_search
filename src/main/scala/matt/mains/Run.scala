package matt.mains
import java.util

import matt.POI

import org.apache.spark.sql.SparkSession
import matt.definitions.GridIndexer
import org.locationtech.jts.geom.{GeometryFactory, PrecisionModel}
import matt.definitions.TableDefs
import matt.definitions.Generic
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.storage.StorageLevel


object Run {
 def main(args: Array[String]) {
  System.out.println("******START-------------------------------------------------------------------------------------------------")
  ///////Param & Config
  //////////////////////////
  Logger.getLogger("org").setLevel(Level.OFF)
  val spark = SparkSession
    .builder
  //    .master("local[*]")
    .appName("Simple Application")
    .config("spark.dynamicAllocation.minExecutors", "25")
    .config("spark.dynamicAllocation.executorIdleTimeout", "50000s")
    .config("spark.driver.port", "51810")
    .config("spark.fileserver.port", "51811")
    .config("spark.broadcast.port", "51812")
    .config("spark.replClassServer.port", "51813")
    .config("spark.blockManager.port", "51814")
    .config("spark.executor.port", "51815")
    .config("spark.network.timeout", "600000s")
    .config("spark.executor.heartbeatInterval", "100000s")
    .config("spark.shuffle.blockTransferService", "nio")
    .config("spark.worker.cleanup.enabled", "true")
    .getOrCreate()
  val hadoopConfig: Configuration = spark.sparkContext.hadoopConfiguration
  hadoopConfig.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
  hadoopConfig.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
  val poiInputFile1 = "~/osmpois-planet-cleaned.csv";
  val poiInputFile = "/home/hamid/8" + ".csv";

  val topk = args(0).toInt;
  val eps = args(1).toDouble
  val cores = args(2).toInt
  val algo=args(3).toInt
  val base=args(4).toInt


  //////end Param & config
  //////////////////////////////////////

  //////Read and split CSV coordination to (nodeNumber, POI) (assign poi to each worker)
  ///////////////////////////////////////////////////////////////

  val inputData = spark.read.format("csv").option("header", "true").option("delimiter", ";").schema(TableDefs.customSchema2).load("hdfs:///input5.csv").drop().filter(x => (x.getAs[Double]("longtitude") != null && x.getAs[Double]("latitude") != null))
  //  .filter(x => (x.getAs[Double]("longtitude") > -10 && x.getAs[Double]("longtitude") < 35)).filter(x => (x.getAs[Double]("latitude") > 35 && x.getAs[Double]("latitude") < 80))//.filter(x => (x.getAs[Double]("longtitude")> -0.489 && x.getAs[Double]("longtitude")< 0.236)).filter(x => (x.getAs[Double]("latitude")> 51.28 && x.getAs[Double]("latitude")< 51.686));//;
  //var inputData = spark.read.format("csv").option("header", "true").option("delimiter", ";").schema(TableDefs.customSchema2).load(poiInputFile).drop().filter(x => (x.getAs[Double]("longtitude") != null && x.getAs[Double]("latitude") != null))//.filter(x => (x.getAs[Double]("longtitude") > -10 && x.getAs[Double]("longtitude") < 35)).filter(x => (x.getAs[Double]("latitude") > 35 && x.getAs[Double]("latitude") < 80))//.filter(x => (x.getAs[Double]("longtitude") > -0.489 && x.getAs[Double]("longtitude") < 0.236)).filter(x => (x.getAs[Double]("latitude") > 51.28 && x.getAs[Double]("latitude") < 51.686));
    // .filter(x => (x.getAs[Double]("longtitude") > -10 && x.getAs[Double]("longtitude") < 35)).filter(x => (x.getAs[Double]("latitude") > 35 && x.getAs[Double]("latitude") < 80))
  val minLong = inputData.select("longtitude").reduce((x, y) => if (x.getAs[Double]("longtitude") < y.getAs[Double]("longtitude")) x else y).getAs[Double](0)
  val maxLong = inputData.select("longtitude").reduce((x, y) => if (x.getAs[Double]("longtitude") > y.getAs[Double]("longtitude")) x else y).getAs[Double](0)
  val minLat = inputData.select("latitude").reduce((x, y) => if (x.getAs[Double]("latitude") < y.getAs[Double]("latitude")) x else y).getAs[Double](0)
  val maxLat = inputData.select("latitude").reduce((x, y) => if (x.getAs[Double]("latitude") > y.getAs[Double]("latitude")) x else y).getAs[Double](0)

  val minmaxLong = (minLong - eps / 10, maxLong + eps / 10);
  println("minmaxLONG: " + minmaxLong);
  val minmaxLat = (minLat - eps / 10, maxLat + eps / 10);
  println("minmaxLat: " + minmaxLat);
  println("topK: " + topk );
  println("eps: " + eps );
  println("part#: " + cores );
  println("base: " + base );



  // find to which node does each point belongs to : (NodeNo,Row)
  val width = math.sqrt(cores).toInt
  val gridIndexer = new GridIndexer(width, eps, minmaxLong, minmaxLat)
  println("partition per cell:" + gridIndexer.gridSizePerCell)
  val geometryFactory = new GeometryFactory(new PrecisionModel(), 4326);
  val nodeToPoint = inputData.rdd.flatMap(x => Generic.poiToKeyValue(x, geometryFactory, gridIndexer));
  nodeToPoint.persist(StorageLevel.MEMORY_AND_DISK);

  ///////Start
  //////////////////////////////

  if (algo==0) {
   val t = System.nanoTime()
   matt.distrib.NstepAlgo.Run(nodeToPoint, eps, topk, gridIndexer,base);
   println("Nstep:::       time:" + (System.nanoTime() - t) / 1000000000 + "s          eps:" + eps + "       topk:" + topk + "     cores:" + cores)
   println("-----------------------------------------------------------------------------------------------------------------------------")
   println("-----------------------------------------------------------------------------------------------------------------------------")
  }
  if (algo==1) {
   val t = System.nanoTime()
   println("sigma 0.9")
   matt.distrib.NstepAlgoApp.Run(nodeToPoint, eps, topk, 0.9, gridIndexer);
   println("sigma 0.7")
   matt.distrib.NstepAlgoApp.Run(nodeToPoint, eps, topk, 0.7, gridIndexer);
   println("sigma 0.5")
   matt.distrib.NstepAlgoApp.Run(nodeToPoint, eps, topk, 0.5, gridIndexer);
   println("NstepApp:::       time:" + (System.nanoTime() - t) / 1000000000 + "s          eps:" + eps + "       topk:" + topk + "     cores:" + cores)
   println("-----------------------------------------------------------------------------------------------------------------------------")
   println("-----------------------------------------------------------------------------------------------------------------------------")
  }
  if (algo==2) {
   val t = System.nanoTime()
   val nodeOptToPoint = inputData.rdd.flatMap(x => Generic.poiOptToKeyValue(x, geometryFactory, gridIndexer));
   matt.distrib.OnestepAlgoOptimized.Run(nodeOptToPoint, eps,  topk, gridIndexer)
   println("SingleOpt:::       time:" + (System.nanoTime() - t) / 1000000000 + "s          eps:" + eps + "       topk:" + topk + "     cores:" + cores)
   println("-----------------------------------------------------------------------------------------------------------------------------")
   println("-----------------------------------------------------------------------------------------------------------------------------")
  }
  if (algo==3) {
   var t = System.nanoTime()
   matt.distrib.OnestepAlgoReduce.Run(nodeToPoint, eps, topk, gridIndexer,base);
   println("Single:::       time:" + (System.nanoTime() - t) / 1000000000 + "s          eps:" + eps + "       topk:" + topk + "     cores:" + cores)
   println("-----------------------------------------------------------------------------------------------------------------------------")
   println("-----------------------------------------------------------------------------------------------------------------------------")
  }

  spark.stop()
 };

 def mergeStat(input: Iterable[POI]): Int = {
  val temp = new util.HashMap[String, POI]
  for (poi <- input) {
   val x = poi.getPoint.getX
   val y = poi.getPoint.getY
   if (temp.containsValue(x + ":" + y)) temp.get(x + ":" + y).increaseScore()
   else temp.put(x + ":" + y, poi)
  }
  temp.size()
 }


 def RoundStat(part: Int, pois: Iterable[POI], round: Int): (Int, Int) = {
  if (pois.size > 5000)
   System.err.println("poi# in a partition " + pois.size + " on location around   " + pois.head.getPoint.getX + ":" + pois.head.getPoint.getY)
  val temp = new util.HashMap[String, POI]
  for (poi <- pois) {
   val x = poi.getPoint.getX
   val y = poi.getPoint.getY
   if (temp.containsValue(x + ":" + y))
    temp.get(x + ":" + y).increaseScore()
   else temp.put(x + ":" + y, poi)
  }
  (pois.size, temp.size())
 }


}