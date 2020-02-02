package matt.mains
import java.util
import java.util.{ArrayList, HashMap}

import matt.{POI, SpatialObject}
import org.apache.spark.sql.SparkSession
import matt.definitions.GridIndexer
import org.locationtech.jts.geom.{Geometry, GeometryFactory, PrecisionModel}
import matt.definitions.TableDefs
import matt.definitions.Generic
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.ListBuffer


object Run {
 def main(args: Array[String]) {
  System.out.println("******47START-------------------------------------------------------------------------------------------------")
  ///////Param & Config
  //////////////////////////
  Logger.getLogger("org").setLevel(Level.OFF)
  val spark = SparkSession
    .builder


    .master("local[*]")
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

  val topk = args(0).toInt;
  val eps = args(1).toDouble
  val cores = args(2).toInt
  val algo = args(3).toInt
  val base = args(4).toInt
  val Kprime = args(5).toInt
  val f = args(6).toString
  val keyword=args(7).toString
  val input = args(8)
  //if(algo==4)


  //////end Param & config
  //////////////////////////////////////

  //////Read and split CSV coordination to (nodeNumber, POI) (assign poi to each worker)
  ///////////////////////////////////////////////////////////////
  var inputData = spark.read.format("com.databricks.spark.csv").option("header", "true")
    .option("inferSchema", "true").option("delimiter", ";").option("nullValue", "null").load(input)
    .drop().filter(x => (x.getAs[Double]("longtitude") != null && x.getAs[Double]("latitude") != null))
  if(keyword!="null") {
   inputData = inputData.filter(x => (x.getAs[String]("keywords") != null && x.getAs[String]("keywords").contains(keyword)))
   if (inputData.count() == 0) {
    println("0:0;POLYGON ((0 0, 0 0, 0 0, 0 0, 0 0));0.0")
    println("JobDone")
    spark.stop()
    return
   }
  }
  //val inputData = spark.read.format("csv").option("header", "true").option("delimiter", ";").schema(TableDefs.customSchema3).load(input).drop().filter(x => (x.getAs[Double]("longtitude") != null && x.getAs[Double]("latitude") != null))
  //  .filter(x => (x.getAs[Double]("longtitude") > -10 && x.getAs[Double]("longtitude") < 35)).filter(x => (x.getAs[Double]("latitude") > 35 && x.getAs[Double]("latitude") < 80))//.filter(x => (x.getAs[Double]("longtitude")> -0.489 && x.getAs[Double]("longtitude")< 0.236)).filter(x => (x.getAs[Double]("latitude")> 51.28 && x.getAs[Double]("latitude")< 51.686));//;
  //var inputData = spark.read.format("csv").option("header", "true").option("delimiter", ";").schema(TableDefs.customSchema2).load(poiInputFile).drop().filter(x => (x.getAs[Double]("longtitude") != null && x.getAs[Double]("latitude") != null))//.filter(x => (x.getAs[Double]("longtitude") > -10 && x.getAs[Double]("longtitude") < 35)).filter(x => (x.getAs[Double]("latitude") > 35 && x.getAs[Double]("latitude") < 80))//.filter(x => (x.getAs[Double]("longtitude") > -0.489 && x.getAs[Double]("longtitude") < 0.236)).filter(x => (x.getAs[Double]("latitude") > 51.28 && x.getAs[Double]("latitude") < 51.686));
  //  .filter(x => (x.getAs[Double]("longtitude") > -10 && x.getAs[Double]("longtitude") < 35)).filter(x => (x.getAs[Double]("latitude") > 35 && x.getAs[Double]("latitude") < 80))
  val minLong = inputData.select("longtitude").reduce((x, y) => if (x.getAs[Double]("longtitude") < y.getAs[Double]("longtitude")) x else y).getAs[Double](0)
  val maxLong = inputData.select("longtitude").reduce((x, y) => if (x.getAs[Double]("longtitude") > y.getAs[Double]("longtitude")) x else y).getAs[Double](0)
  val minLat = inputData.select("latitude").reduce((x, y) => if (x.getAs[Double]("latitude") < y.getAs[Double]("latitude")) x else y).getAs[Double](0)
  val maxLat = inputData.select("latitude").reduce((x, y) => if (x.getAs[Double]("latitude") > y.getAs[Double]("latitude")) x else y).getAs[Double](0)
 // val minLong = inputData.select("lon").reduce((x, y) => if (x.getAs[Double]("lon") < y.getAs[Double]("lon")) x else y).getAs[Double](0)
 // val maxLong = inputData.select("lon").reduce((x, y) => if (x.getAs[Double]("lon") > y.getAs[Double]("lon")) x else y).getAs[Double](0)
 // val minLat = inputData.select("lat").reduce((x, y) => if (x.getAs[Double]("lat") < y.getAs[Double]("lat")) x else y).getAs[Double](0)
 // val maxLat = inputData.select("lat").reduce((x, y) => if (x.getAs[Double]("lat") > y.getAs[Double]("lat")) x else y).getAs[Double](0)

  val minmaxLong = (minLong - eps / 100.0, maxLong + eps / 100.0);
  println("minmaxLONG: " + minmaxLong);
  val minmaxLat = (minLat - eps / 100.0, maxLat + eps / 100.0);
  println("minmaxLat: " + minmaxLat);
  println("topK: " + topk);
  println("eps: " + eps);
  println("part#: " + cores);
  println("algo: " + algo);
  println("base: " + base);
  println("k': " + Kprime);
  println("targetCal: "+f)
  println("keyword: "+keyword)




  // find to which node does each point belongs to : (NodeNo,Row)
  val width = math.sqrt(cores).toInt
  val gridIndexer = new GridIndexer(width, eps, minmaxLong, minmaxLat)
  println("partition per cell:" + gridIndexer.gridSizePerCell)
  val geometryFactory = new GeometryFactory(new PrecisionModel(), 4326);

  ///////Start
  //////////////////////////////

  if (algo==0) {//MR
    val nodeToPoint = inputData.rdd.flatMap(x => Generic.poiToKeyValue(x, geometryFactory, gridIndexer,f));
    nodeToPoint.persist(StorageLevel.MEMORY_AND_DISK);
   val t = System.nanoTime()
   matt.distrib.NstepAlgo.Run(nodeToPoint, eps, topk,Kprime, gridIndexer,base);
   println("Nstep:::       time:" + (System.nanoTime() - t) / 1000000000 + "s          eps:" + eps + "       topk:" + topk + "     cores:" + cores)
  }
  if (algo == 1) {//MR Approximate
   val nodeToPoint = inputData.rdd.flatMap(x => Generic.poiToKeyValue(x, geometryFactory, gridIndexer,f));
   nodeToPoint.persist(StorageLevel.MEMORY_AND_DISK);
   val t = System.nanoTime()
   println("sigma 0.9")
   matt.distrib.NstepAlgoApp.Run(nodeToPoint, eps, topk, 0.9, gridIndexer);
   println("sigma 0.7")
   matt.distrib.NstepAlgoApp.Run(nodeToPoint, eps, topk, 0.7, gridIndexer);
   println("sigma 0.5")
   matt.distrib.NstepAlgoApp.Run(nodeToPoint, eps, topk, 0.5, gridIndexer);
   println("NstepApp:::       time:" + (System.nanoTime() - t) / 1000000000 + "s          eps:" + eps + "       topk:" + topk + "     cores:" + cores)
  }
  if (algo == 2) { //MR + region upper bound
   val t = System.nanoTime()
   val nodeOptToPoint = inputData.rdd.flatMap(x => Generic.poiOptToKeyValue(x, geometryFactory, gridIndexer));
   nodeOptToPoint.persist(StorageLevel.MEMORY_AND_DISK);
   matt.distrib.OnestepAlgoOptimized.Run(nodeOptToPoint, eps, topk, gridIndexer, base, 1)
   println("SingleOpt1(Dim):::       time:" + (System.nanoTime() - t) / 1000000000 + "s          eps:" + eps + "       topk:" + topk + "     cores:" + cores)
  }
  if (algo == 3) { //SR
   val nodeToPoint = inputData.rdd.flatMap(x => Generic.poiToKeyValue(x, geometryFactory, gridIndexer,f));
   nodeToPoint.persist(StorageLevel.MEMORY_AND_DISK);
   val t = System.nanoTime()
   matt.distrib.OnestepAlgoReduce.Run(nodeToPoint, eps, topk, gridIndexer, base);
   println("Single:::       time:" + (System.nanoTime() - t) / 1000000000 + "s          eps:" + eps + "       topk:" + topk + "     cores:" + cores)
  }

  if (algo == 4) { //Hybrid
   val nodeToPoint = inputData.rdd.flatMap(x => Generic.poiToKeyValue(x, geometryFactory, gridIndexer,f));
   nodeToPoint.persist(StorageLevel.MEMORY_AND_DISK);
   val t = System.nanoTime()
   matt.distrib.OnestepAlgoReduceHybrid.Run(nodeToPoint, eps, topk, gridIndexer, base, Kprime);
   println("Hybrid:::       time:" + (System.nanoTime() - t) / 1000000000 + "s          eps:" + eps + "       topk:" + topk + "     cores:" + cores)
  }
  if (algo == 5) {//SR + cell upper score
   val t = System.nanoTime()
   val nodeOptToPoint = inputData.rdd.flatMap(x => Generic.poiOptToKeyValue(x, geometryFactory, gridIndexer));
    nodeOptToPoint.persist(StorageLevel.MEMORY_AND_DISK);

    matt.distrib.OnestepAlgoOptimized.Run(nodeOptToPoint, eps,  topk, gridIndexer,base,2)
   println("SingleOpt2(Res):::       time:" + (System.nanoTime() - t) / 1000000000 + "s          eps:" + eps + "       topk:" + topk + "     cores:" + cores)
  }
  if (algo==6) {//Hybrid+ send to some partitions
   val nodeToPoint = inputData.rdd.flatMap(x => Generic.poiToKeyValue(x, geometryFactory, gridIndexer,f));
   nodeToPoint.persist(StorageLevel.MEMORY_AND_DISK);
   val t = System.nanoTime()
   matt.distrib.OnestepAlgoReduceHybridOpt.Run(nodeToPoint, eps, topk, gridIndexer,base,Kprime);
   println("HybridOpt:::       time:" + (System.nanoTime() - t) / 1000000000 + "s          eps:" + eps + "       topk:" + topk + "     cores:" + cores)
  }
  if (algo == 7) {//Hybrid+ send to some partitions+ region upper bound
   val nodeOptToPoint = inputData.rdd.flatMap(x => Generic.poiOptToKeyValue(x, geometryFactory, gridIndexer));
   nodeOptToPoint.persist(StorageLevel.MEMORY_AND_DISK);
   val t = System.nanoTime()
   matt.distrib.OnestepAlgoReduceHybridOptOpt.Run(nodeOptToPoint, eps, topk, gridIndexer, base, Kprime, 1);
   println("HybridOpt(Dim):::       time:" + (System.nanoTime() - t) / 1000000000 + "s          eps:" + eps + "       topk:" + topk + "     cores:" + cores)
  }
  if (algo == 8) {//Hybrid+ send to some partitions+ cell upper bound
   val nodeOptToPoint = inputData.rdd.flatMap(x => Generic.poiOptToKeyValue(x, geometryFactory, gridIndexer));
   nodeOptToPoint.persist(StorageLevel.MEMORY_AND_DISK);
   val t = System.nanoTime()
   matt.distrib.OnestepAlgoReduceHybridOptOpt.Run(nodeOptToPoint, eps, topk, gridIndexer, base, Kprime, 2);
   println("HybridOpt(Res):::       time:" + (System.nanoTime() - t) / 1000000000 + "s          eps:" + eps + "       topk:" + topk + "     cores:" + cores)
  }
  println("JobDone")
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


 def RoundStat(part: Int, pois: Iterable[POI]): Seq[POI] = {
  val temp: HashMap[String,POI] = new HashMap[String,POI]
  for (poi <- pois) {
   val x = (poi.getPoint.getX * 100000).toInt
   val y = (poi.getPoint.getY * 100000).toInt
   if (temp.containsKey(x + ":" + y))
    temp.get(x + ":" + y).increaseScore()
   else
    temp.put(x + ":" + y, poi)

  }
  val res=new ListBuffer[POI]()
  val it=temp.values().iterator()
  while(it.hasNext)
   res.+=(it.next())
  res
 }
}