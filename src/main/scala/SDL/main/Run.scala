package SDL.main

import java.util
import java.util.HashMap

import SDL.POI
import SDL.definitions.{Generic, GridIndexer}
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.{BasicConfigurator, Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.locationtech.jts.geom.{GeometryFactory, PrecisionModel}

import scala.collection.mutable.ListBuffer


object Run {
  def main(args: Array[String]) {
    ///////Param & Config
    //////////////////////////
    // val pathToCSVs="/home/hamid/"
    val sparkServer=args(0)
    val topk = args(1).toInt;
    val eps = args(2).toDouble
    val partitionsCNT = args(3).toInt
    val algo = args(4).toInt
    val base = args(5).toInt
    val Kprime = args(6).toInt
    val f = args(7).toString
    val keywordsColumn = args(8).toString
    val keywords = args(9).toString
    val input = args(10)
    val dist = args(11).toBoolean

    var keywords2=""
    var keywordsColumn2=""
    if(args.size==14){
      keywordsColumn2=args(12)
      keywords2=args(13)
    }
    val csv_delimiter = "," //args(10)
    val keyword_delimiter = ";" //args(11)
    BasicConfigurator.configure()
    Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession
      .builder
      //.master(sparkServer)
      .appName("BRS")
      .config("spark.dynamicAllocation.minExecutors", "25")
      .config("spark.dynamicAllocation.executorIdleTimeout", "50000s")
      .config("spark.network.timeout", "600000s")
      .config("spark.executor.heartbeatInterval", "100000s")
      .config("spark.worker.cleanup.enabled", "true")
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")
    val hadoopConfig: Configuration = spark.sparkContext.hadoopConfiguration
    hadoopConfig.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
    hadoopConfig.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)


    System.err.println("Start;" + ";" + topk + ";" + eps + ";" + algo + ";" + Kprime + ";" + f + ";" + keywordsColumn + ";" + keywords + ";" + input)
    //////Read and split CSV coordination to (nodeNumber, POI) (assign poi to each worker)
    ///////////////////////////////////////////////////////////////
    if (topk <= 0 || eps <= 0 || partitionsCNT <= 0 || algo > 9  || algo < 0 || base <= 0 || Kprime <= 0)
      throw new Exception("err : Invalid input ranges!!!")
    var inputData = spark.read.format("com.databricks.spark.csv").option("header", "true")
      .option("inferSchema", "true").option("delimiter", csv_delimiter).option("nullValue", "null").load(input) //.sample(0.01)
      .drop().filter(x => (x.getAs[Double]("lon") != null && x.getAs[Double]("lat") != null))
    //.filter(x => (x.getAs[Double]("lon") > 8.1 && x.getAs[Double]("lon") < 10.6)).filter(x => (x.getAs[Double]("lat") > 44.2 && x.getAs[Double]("lat") < 45.6)).sample(0.1,12345456)
    val keywordSplited = keywords.split(keyword_delimiter)
    val keywordSplited2 = keywords2.split(keyword_delimiter)
    if (keywordsColumn != "null") {
      inputData = inputData.filter(x => x.getAs[String](keywordsColumn) != null && {
        var f = false
        keywordSplited.foreach(y => if (x.getAs[String](keywordsColumn).split(keyword_delimiter).find(_.equals(y)).isDefined) f = true)
        f
      })
    }

    if (keywordsColumn2 != "") {
      inputData = inputData.filter(x => x.getAs[String](keywordsColumn2) != null && {
        var f = false
        keywordSplited2.foreach(y => if (x.getAs[String](keywordsColumn2).split(keyword_delimiter).find(_.equals(y)).isDefined) f = true)
        f
      })
    }
    inputData = if (f != "null") inputData.select( "lon", "lat", f).filter(x => x.getAs(f) != null)
    else inputData.select("lon", "lat")
    if (inputData.count() == 0) {
      println("[\n{\n}\n]")
      System.err.println("JobDone")
      spark.stop()
      return
    }
    val minLong = inputData.select("lon").reduce((x, y) => if (x.getAs[Double]("lon") < y.getAs[Double]("lon")) x else y).getAs[Double](0)
    val maxLong = inputData.select("lon").reduce((x, y) => if (x.getAs[Double]("lon") > y.getAs[Double]("lon")) x else y).getAs[Double](0)
    val minLat = inputData.select("lat").reduce((x, y) => if (x.getAs[Double]("lat") < y.getAs[Double]("lat")) x else y).getAs[Double](0)
    val maxLat = inputData.select("lat").reduce((x, y) => if (x.getAs[Double]("lat") > y.getAs[Double]("lat")) x else y).getAs[Double](0)
    val minmaxLong = (minLong, maxLong );
    val minmaxLat = (minLat, maxLat );
    // find to which node does each point belongs to : (NodeNo,Row)
    val width = math.sqrt(partitionsCNT).toInt
    val gridIndexer = new GridIndexer(width, eps, minmaxLong, minmaxLat)
    val geometryFactory = new GeometryFactory(new PrecisionModel(), 4326);
    ///////Start
    //////////////////////////////
    if (algo == 9) { //MR
      var result="["
      val regions = inputData.select("lon","lat").map(coor=> (((coor.getDouble(0)-minmaxLong._1)/eps).toInt+","+((coor.getDouble(1)-minmaxLat._1)/eps).toInt,1))
        .rdd.reduceByKey(_+_).sortBy(_._2,false).take(topk).toList
      for(i<- 1 to topk){
        result+=("\n{\n\"rank\":"+i+",\n\"center\":["+(regions(i-1)._1.split(",")(0).toInt*eps+eps/2.0+minmaxLong._1)
          +","+(regions(i-1)._1.split(",")(1).toInt*eps+eps/2.0+minmaxLat._1)+"],\n\"score\":"+regions(i-1)._2+"\n},")
      }
      result=result.dropRight(1)
      result+="\n]"
      println(result)
    }
    if (algo == 0) { //MR
      val nodeToPoint = inputData.rdd.flatMap(x => Generic.poiToKeyValue(x, geometryFactory, gridIndexer, f));
      nodeToPoint.persist(StorageLevel.MEMORY_AND_DISK);
      val t = System.nanoTime()
      SDL.distrib.NstepAlgo.Run(nodeToPoint, eps, topk, Kprime, gridIndexer, base,dist);
      //println("Nstep:::       time:" + (System.nanoTime() - t) / 1000000000 + "s          eps:" + eps + "       topk:" + topk + "     partitionsCNT:" + partitionsCNT)
    }
    if (algo == 8) { //MR Approximate
      val nodeToPoint = inputData.rdd.flatMap(x => Generic.poiToKeyValue(x, geometryFactory, gridIndexer, f));
      nodeToPoint.persist(StorageLevel.MEMORY_AND_DISK);
      val t = System.nanoTime()
      //println("sigma 0.9")
      SDL.distrib.NstepAlgoApp.Run(nodeToPoint, eps, topk, 0.9, gridIndexer,dist);
      //println("sigma 0.7")
      SDL.distrib.NstepAlgoApp.Run(nodeToPoint, eps, topk, 0.7, gridIndexer,dist);
      //println("sigma 0.5")
      SDL.distrib.NstepAlgoApp.Run(nodeToPoint, eps, topk, 0.5, gridIndexer,dist);
      // println("NstepApp:::       time:" + (System.nanoTime() - t) / 1000000000 + "s          eps:" + eps + "       topk:" + topk + "     partitionsCNT:" + partitionsCNT)
    }
    if (algo == 3) { //MR + region upper bound
      val t = System.nanoTime()
      val nodeOptToPoint = inputData.rdd.flatMap(x => Generic.poiOptToKeyValue(x, geometryFactory, gridIndexer, f));
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
      val nodeOptToPoint = inputData.rdd.flatMap(x => Generic.poiOptToKeyValue(x, geometryFactory, gridIndexer, f));
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
      val nodeOptToPoint = inputData.rdd.flatMap(x => Generic.poiOptToKeyValue(x, geometryFactory, gridIndexer, f));
      nodeOptToPoint.persist(StorageLevel.MEMORY_AND_DISK);
      val t = System.nanoTime()
      SDL.distrib.OnestepAlgoReduceHybridOptOpt.Run(nodeOptToPoint, eps, topk, gridIndexer, base, Kprime, 1);
      // println("HybridOpt(Dim):::       time:" + (System.nanoTime() - t) / 1000000000 + "s          eps:" + eps + "       topk:" + topk + "     partitionsCNT:" + partitionsCNT)
    }
    if (algo == 2) { //Hybrid+ send to some partitions+ cell upper bound
      val nodeOptToPoint = inputData.rdd.flatMap(x => Generic.poiOptToKeyValue(x, geometryFactory, gridIndexer, f));
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