package SDL.main
import java.util
import java.util.HashMap

import SDL.POI
import org.apache.spark.sql.SparkSession
import SDL.definitions.GridIndexer
import org.locationtech.jts.geom.{GeometryFactory, PrecisionModel}
import SDL.definitions.Generic
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer
//https://hamidshj@bitbucket.org/hamidshj/hamid.git
object Run {
 def main(args: Array[String]) {
//  BRSInvoker.CalBRS("local","/home/hamid/SpatialProject.jar",Seq("").toArray)
  //System.out.println("******51START-------------------------------------------------------------------------------------------------")
  val intTime=System.nanoTime()
  ///////Param & Config
  //////////////////////////
  Logger.getLogger("org").setLevel(Level.OFF)
  val spark = SparkSession
    .builder


    .master("local[*]")
    .appName("Simple Application")
    .config("spark.dynamicAllocation.minExecutors", "25")
    .config("spark.dynamicAllocation.executorIdleTimeout", "50000s")
    .config("spark.network.timeout", "600000s")
    .config("spark.executor.heartbeatInterval", "100000s")
    .config("spark.shuffle.blockTransferService", "nio")
    .config("spark.worker.cleanup.enabled", "true")
    .config("spark.scheduler.mode", "FAIR")
    .getOrCreate()



  val hadoopConfig: Configuration = spark.sparkContext.hadoopConfiguration

  hadoopConfig.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)

  hadoopConfig.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)


  val topk = args(0).toInt;
  val eps = args(1).toDouble
  val partitionsCNT = args(2).toInt
  val algo = args(3).toInt
  val base = args(4).toInt
  val Kprime = args(5).toInt
  val f = args(6).toString
  val keywordsColumn=args(7).toString
  val keywords = args(8).toString
  val input = args(9)
  val csv_delimiter = ";"//args(10)
  val keyword_delimiter = ","//args(11)
  //if(algo==4)


  //////end Param & config
  //////////////////////////////////////

  //////Read and split CSV coordination to (nodeNumber, POI) (assign poi to each worker)
  ///////////////////////////////////////////////////////////////
  if(topk<=0||eps<=0||partitionsCNT<=0||algo>2||algo<0||base<=0||Kprime<=0)
   throw new Exception("Invalid input ranges!!!")
  var inputData = spark.read.format("com.databricks.spark.csv").option("header", "true")
    .option("inferSchema", "true").option("delimiter", csv_delimiter).option("nullValue", "null").load(input)
    .drop().filter(x => (x.getAs[Double]("lon") != null && x.getAs[Double]("lat") != null))
    .filter(x => (x.getAs[Double]("lon") > 8.1 && x.getAs[Double]("lon") < 10.6)).filter(x => (x.getAs[Double]("lat") > 44.2 && x.getAs[Double]("lat") < 45.6)).sample(0.1,12345456)
  inputData.withColumnRenamed("lon","lon");
 // inputData.withColumnRenamed("lan","lat");
  val keywordSplited=keywords.split(keyword_delimiter)
  if (keywordsColumn != "null") {
   inputData = inputData.filter(x => (x.getAs[String](keywordsColumn) != null && {
    var f = false
    keywordSplited.foreach(y => if (x.getAs[String](keywordsColumn).contains(y)) f = true)
    f
   }))
  }



  inputData = if (f != "null") inputData.select("id", "lon", "lat", f).filter(x => x.getAs(f) != null)
  else inputData.select("id", "lon", "lat")
  if (inputData.count() == 0) {
   println("{\n}")
   //println("JobDone")
   spark.stop()
   return
  }
  //val inputData = spark.read.format("csv").option("header", "true").option("delimiter", ";").schema(TableDefs.customSchema3).load(input).drop().filter(x => (x.getAs[Double]("lon") != null && x.getAs[Double]("lat") != null))
  //  .filter(x => (x.getAs[Double]("lon") > -10 && x.getAs[Double]("lon") < 35)).filter(x => (x.getAs[Double]("lat") > 35 && x.getAs[Double]("lat") < 80))//.filter(x => (x.getAs[Double]("lon")> -0.489 && x.getAs[Double]("lon")< 0.236)).filter(x => (x.getAs[Double]("lat")> 51.28 && x.getAs[Double]("lat")< 51.686));//;
  //var inputData = spark.read.format("csv").option("header", "true").option("delimiter", ";").schema(TableDefs.customSchema2).load(poiInputFile).drop().filter(x => (x.getAs[Double]("lon") != null && x.getAs[Double]("lat") != null))//.filter(x => (x.getAs[Double]("lon") > -10 && x.getAs[Double]("lon") < 35)).filter(x => (x.getAs[Double]("lat") > 35 && x.getAs[Double]("lat") < 80))//.filter(x => (x.getAs[Double]("lon") > -0.489 && x.getAs[Double]("lon") < 0.236)).filter(x => (x.getAs[Double]("lat") > 51.28 && x.getAs[Double]("lat") < 51.686));
  //  .filter(x => (x.getAs[Double]("lon") > -10 && x.getAs[Double]("lon") < 35)).filter(x => (x.getAs[Double]("lat") > 35 && x.getAs[Double]("lat") < 80))
  val minLong = inputData.select("lon").reduce((x, y) => if (x.getAs[Double]("lon") < y.getAs[Double]("lon")) x else y).getAs[Double](0)
  val maxLong = inputData.select("lon").reduce((x, y) => if (x.getAs[Double]("lon") > y.getAs[Double]("lon")) x else y).getAs[Double](0)
  val minLat = inputData.select("lat").reduce((x, y) => if (x.getAs[Double]("lat") < y.getAs[Double]("lat")) x else y).getAs[Double](0)
  val maxLat = inputData.select("lat").reduce((x, y) => if (x.getAs[Double]("lat") > y.getAs[Double]("lat")) x else y).getAs[Double](0)
  // val minLong = inputData.select("lon").reduce((x, y) => if (x.getAs[Double]("lon") < y.getAs[Double]("lon")) x else y).getAs[Double](0)
  // val maxLong = inputData.select("lon").reduce((x, y) => if (x.getAs[Double]("lon") > y.getAs[Double]("lon")) x else y).getAs[Double](0)
  // val minLat = inputData.select("lat").reduce((x, y) => if (x.getAs[Double]("lat") < y.getAs[Double]("lat")) x else y).getAs[Double](0)
  // val maxLat = inputData.select("lat").reduce((x, y) => if (x.getAs[Double]("lat") > y.getAs[Double]("lat")) x else y).getAs[Double](0)

  val minmaxLong = (minLong - eps / 100.0, maxLong + eps / 100.0);
  //println("minmaxLONG: " + minmaxLong);
  val minmaxLat = (minLat - eps / 100.0, maxLat + eps / 100.0);
 // println("minmaxLat: " + minmaxLat);
 // val minmaxLong = (7 - eps / 100.0, 19 + eps / 100.0);
 // println("minmaxLONG: " + minmaxLong);
  //val minmaxLat = (36 - eps / 100.0, 47 + eps / 100.0);
//  println("minmaxLat: " + minmaxLat);
  //println("topK: " + topk);
 // println("eps: " + eps);
 // println("part#: " + partitionsCNT);
 // println("algo: " + algo);
 // println("base: " + base);
 // println("k': " + Kprime);
 // println("targetCal: " + f)
 // println("keyword: " + keywords)


  // find to which node does each point belongs to : (NodeNo,Row)
  val width = math.sqrt(partitionsCNT).toInt
  val gridIndexer = new GridIndexer(width, eps, minmaxLong, minmaxLat)
  //println("partition per cell:" + gridIndexer.gridSizePerCell)
  val geometryFactory = new GeometryFactory(new PrecisionModel(), 4326);

  ///////Start
  //////////////////////////////
  //println("PreTime: "+(System.nanoTime() - intTime) / 1000000000+"s   record number: "+inputData.count())
  if (algo == 0) { //MR
   val nodeToPoint = inputData.rdd.flatMap(x => Generic.poiToKeyValue(x, geometryFactory, gridIndexer, f));
   nodeToPoint.persist(StorageLevel.MEMORY_AND_DISK);
   val t = System.nanoTime()
   SDL.distrib.NstepAlgo.Run(nodeToPoint, eps, topk, Kprime, gridIndexer, base);
   //println("Nstep:::       time:" + (System.nanoTime() - t) / 1000000000 + "s          eps:" + eps + "       topk:" + topk + "     partitionsCNT:" + partitionsCNT)
  }
  if (algo == 8) { //MR Approximate
   val nodeToPoint = inputData.rdd.flatMap(x => Generic.poiToKeyValue(x, geometryFactory, gridIndexer, f));
   nodeToPoint.persist(StorageLevel.MEMORY_AND_DISK);
   val t = System.nanoTime()
   //println("sigma 0.9")
   SDL.distrib.NstepAlgoApp.Run(nodeToPoint, eps, topk, 0.9, gridIndexer);
   //println("sigma 0.7")
   SDL.distrib.NstepAlgoApp.Run(nodeToPoint, eps, topk, 0.7, gridIndexer);
   //println("sigma 0.5")
   SDL.distrib.NstepAlgoApp.Run(nodeToPoint, eps, topk, 0.5, gridIndexer);
  // println("NstepApp:::       time:" + (System.nanoTime() - t) / 1000000000 + "s          eps:" + eps + "       topk:" + topk + "     partitionsCNT:" + partitionsCNT)
  }
  if (algo == 3) { //MR + region upper bound
   val t = System.nanoTime()
   val nodeOptToPoint = inputData.rdd.flatMap(x => Generic.poiOptToKeyValue(x, geometryFactory, gridIndexer,f));
   nodeOptToPoint.persist(StorageLevel.MEMORY_AND_DISK);
   SDL.distrib.OnestepAlgoOptimized.Run(nodeOptToPoint, eps, topk, gridIndexer, base, 1)
   //println("SingleOpt1(Dim):::       time:" + (System.nanoTime() - t) / 1000000000 + "s          eps:" + eps + "       topk:" + topk + "     partitionsCNT:" + partitionsCNT)
  }
  if (algo == 5) { //SR
   val nodeToPoint = inputData.rdd.flatMap(x => Generic.poiToKeyValue(x, geometryFactory, gridIndexer, f));
   nodeToPoint.persist(StorageLevel.MEMORY_AND_DISK);
   val t = System.nanoTime()
   SDL.distrib.OnestepAlgoReduce.Run(nodeToPoint, eps, topk, gridIndexer, base);
  // println("Single:::       time:" + (System.nanoTime() - t) / 1000000000 + "s          eps:" + eps + "       topk:" + topk + "     partitionsCNT:" + partitionsCNT)
  }

  if (algo == 4) { //Hybrid
   val nodeToPoint = inputData.rdd.flatMap(x => Generic.poiToKeyValue(x, geometryFactory, gridIndexer, f));
   nodeToPoint.persist(StorageLevel.MEMORY_AND_DISK);
   val t = System.nanoTime()
   SDL.distrib.OnestepAlgoReduceHybrid.Run(nodeToPoint, eps, topk, gridIndexer, base, Kprime);
  // println("Hybrid:::       time:" + (System.nanoTime() - t) / 1000000000 + "s          eps:" + eps + "       topk:" + topk + "     partitionsCNT:" + partitionsCNT)
  }
  if (algo == 1) { //SR + cell upper score
   val t = System.nanoTime()
   val nodeOptToPoint = inputData.rdd.flatMap(x => Generic.poiOptToKeyValue(x, geometryFactory, gridIndexer,f));
   nodeOptToPoint.persist(StorageLevel.MEMORY_AND_DISK);

   SDL.distrib.OnestepAlgoOptimized.Run(nodeOptToPoint, eps, topk, gridIndexer, base, 2)
   //println("SingleOpt2(Res):::       time:" + (System.nanoTime() - t) / 1000000000 + "s          eps:" + eps + "       topk:" + topk + "     partitionsCNT:" + partitionsCNT)
  }
  if (algo == 6) { //Hybrid+ send to some partitions
   val nodeToPoint = inputData.rdd.flatMap(x => Generic.poiToKeyValue(x, geometryFactory, gridIndexer, f));
   nodeToPoint.persist(StorageLevel.MEMORY_AND_DISK);
   val t = System.nanoTime()
   SDL.distrib.OnestepAlgoReduceHybridOpt.Run(nodeToPoint, eps, topk, gridIndexer, base, Kprime);
  // println("HybridOpt:::       time:" + (System.nanoTime() - t) / 1000000000 + "s          eps:" + eps + "       topk:" + topk + "     partitionsCNT:" + partitionsCNT)
  }
  if (algo == 7) { //Hybrid+ send to some partitions+ region upper bound
   val nodeOptToPoint = inputData.rdd.flatMap(x => Generic.poiOptToKeyValue(x, geometryFactory, gridIndexer,f));
   nodeOptToPoint.persist(StorageLevel.MEMORY_AND_DISK);
   val t = System.nanoTime()
   SDL.distrib.OnestepAlgoReduceHybridOptOpt.Run(nodeOptToPoint, eps, topk, gridIndexer, base, Kprime, 1);
  // println("HybridOpt(Dim):::       time:" + (System.nanoTime() - t) / 1000000000 + "s          eps:" + eps + "       topk:" + topk + "     partitionsCNT:" + partitionsCNT)
  }
  if (algo == 2) { //Hybrid+ send to some partitions+ cell upper bound
   val nodeOptToPoint = inputData.rdd.flatMap(x => Generic.poiOptToKeyValue(x, geometryFactory, gridIndexer,f));
   nodeOptToPoint.persist(StorageLevel.MEMORY_AND_DISK);
   val t = System.nanoTime()
   SDL.distrib.OnestepAlgoReduceHybridOptOpt.Run(nodeOptToPoint, eps, topk, gridIndexer, base, Kprime, 2);
  // println("HybridOpt(Res):::       time:" + (System.nanoTime() - t) / 1000000000 + "s          eps:" + eps + "       topk:" + topk + "     partitionsCNT:" + partitionsCNT)
  }
  //println("JobDone")
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
  val temp: HashMap[String, POI] = new HashMap[String, POI]
  for (poi <- pois) {
   val x = (poi.getPoint.getX * 100000).toInt
   val y = (poi.getPoint.getY * 100000).toInt
   if (temp.containsKey(x + ":" + y))
    temp.get(x + ":" + y).increaseScore()
   else
    temp.put(x + ":" + y, poi)

  }
  val res = new ListBuffer[POI]()
  val it = temp.values().iterator()
  while (it.hasNext)
   res.+=(it.next())
  res
 }
}