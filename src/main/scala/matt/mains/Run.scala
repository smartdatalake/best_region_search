package matt.mains
import scala.math.pow
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
  //  .master("local[*]")
    .appName("Simple Application")
    .config("spark.driver.port", "51810")
    .config("spark.fileserver.port", "51811")
    .config("spark.broadcast.port", "51812")
    .config("spark.replClassServer.port", "51813")
    .config("spark.blockManager.port", "51814")
    .config("spark.executor.port", "51815")
    //  .config("spark.executor.memory", "7g")
  //  .config("spark.driver.memory", "7g")
    .config("spark.network.timeout", "60000s")
    .config("spark.executor.heartbeatInterval", "10000s")
    .config("spark.shuffle.blockTransferService", "nio")
    .config("spark.worker.cleanup.enabled", "true")
  //  .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()
  val hadoopConfig: Configuration = spark.sparkContext.hadoopConfiguration
  hadoopConfig.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
  hadoopConfig.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
  val poiInputFile = "~/osmpois-planet-cleaned.csv";
  val poiInputFile9898 = "/home/hamid/5" + ".csv";
  val poiInputFile6565 = "/home/hamid/reducedFlickr.csv";
  val poiInputFile3 = "/home/hamid/input.csv";

  var eps = 0.0001
  val topk = 300
  val decayConstant = 0.7


   //////end Param & config
   //////////////////////////////////////

   //////Read and split CSV coordination to (nodeNumber, POI) (assign poi to each worker)
   ///////////////////////////////////////////////////////////////

   val inputData = spark.read.format("csv").option("header", "true").option("delimiter", ";").schema(TableDefs.customSchema2).load("hdfs:///input2.csv").drop().filter(x => (x.getAs[Double]("longtitude") != null && x.getAs[Double]("latitude") != null))
     .filter(x => (x.getAs[Double]("longtitude") > -10 && x.getAs[Double]("longtitude") < 35)).filter(x => (x.getAs[Double]("latitude") > 35 && x.getAs[Double]("latitude") < 80))////.filter(x => (x.getAs[Double]("longtitude")> -0.489 && x.getAs[Double]("longtitude")< 0.236)).filter(x => (x.getAs[Double]("latitude")> 51.28 && x.getAs[Double]("latitude")< 51.686));//;
   // val inputData = spark.read.format("csv").option("header", "true").option("delimiter", ";").schema(TableDefs.customSchema2).load("hdfs:///input2.csv").drop().filter(x => (x.getAs[Double]("longtitude") != null && x.getAs[Double]("latitude") != null)).filter(x => (x.getAs[Double]("longtitude")> 3 && x.getAs[Double]("longtitude")< 12)).filter(x => (x.getAs[Double]("latitude")> 44 && x.getAs[Double]("latitude")< 53));
   //var inputData = spark.read.format("csv").option("header", "true").option("delimiter", ";").schema(TableDefs.customSchema2).load(poiInputFile).drop().filter(x => (x.getAs[Double]("longtitude") != null && x.getAs[Double]("latitude") != null))//.filter(x => (x.getAs[Double]("longtitude") > -0.489 && x.getAs[Double]("longtitude") < 0.236)).filter(x => (x.getAs[Double]("latitude") > 51.28 && x.getAs[Double]("latitude") < 51.686));

   val minLong = inputData.select("longtitude").reduce((x, y) => if (x.getAs[Double]("longtitude") < y.getAs[Double]("longtitude")) x else y).getAs[Double](0)
   val maxLong = inputData.select("longtitude").reduce((x, y) => if (x.getAs[Double]("longtitude") > y.getAs[Double]("longtitude")) x else y).getAs[Double](0)
   val minLat = inputData.select("latitude").reduce((x, y) => if (x.getAs[Double]("latitude") < y.getAs[Double]("latitude")) x else y).getAs[Double](0)
   val maxLat = inputData.select("latitude").reduce((x, y) => if (x.getAs[Double]("latitude") > y.getAs[Double]("latitude")) x else y).getAs[Double](0)

   val minmaxLong = (minLong - eps / 10, maxLong + eps / 10);
   println("\n\nminmaxLONG: " + minmaxLong + "\n\n");
   val minmaxLat = (minLat - eps / 10, maxLat + eps / 10);
   println("\n\nminmaxLat: " + minmaxLat + "\n\n");
 //  println("All POI Size:" + inputData.collect().size)

   // find to which node does each point belongs to : (NodeNo,Row)
  for (eps<-Set(0.001,0.002)) {
   val dataSize = math.max((minmaxLat._2 - minmaxLat._1), (minmaxLong._2 - minmaxLong._1))
   val cellSize = eps.asInstanceOf[Double]
   val p = cellSize / dataSize.asInstanceOf[Double]
   val dataSizePerCell = math.floor(dataSize / cellSize.asInstanceOf[Double]).toInt
   val width = math.ceil(dataSizePerCell / 78).toInt
   val cores = width*width
   val gridIndexer = new GridIndexer(width, eps, minmaxLong, minmaxLat)
   val shift = gridIndexer.gridSizePerCell * gridIndexer.cellSize
   println("cells per partition:" + gridIndexer.gridSizePerCell)
   val geometryFactory = new GeometryFactory(new PrecisionModel(), 4326);
   val nodeToPoint = inputData.rdd.flatMap(x => Generic.poiToKeyValue(x, geometryFactory, gridIndexer));
   // nodeToPoint.map(x=>gridIndexer.getCellIndex(x._2.getPoint.getX,x._2.getPoint.getY)).groupBy(l => l).map(t => (t._1, t._2.toList.size)).filter(x=>x._2>1000).foreach(println)

   ////////End Read & split data poi to each worker
   //////////////////////////////////////////////////////////////////////////////


   ///////Start
   //////////////////////////////
   val Nstep = true;
   val Nstep2 = false
   val OneStep = true;
   val OneStepOptimized = false

   if (Nstep) {
    val t = System.nanoTime()
    println("Nstep:::       time:" + (System.nanoTime() - t) / 1000000000 + "s          eps:" + eps + "       topk:" + topk + "     cores:" + cores)
    matt.distrib.NstepAlgo.Run(nodeToPoint, eps, topk);
    println("Nstep:::       time:" + (System.nanoTime() - t) / 1000000000 + "s          eps:" + eps + "       topk:" + topk + "     cores:" + cores)
    println("-----------------------------------------------------------------------------------------------------------------------------")
    println("-----------------------------------------------------------------------------------------------------------------------------")
   }

   /* if (Nstep2) {
   matt.distrib.NstepAlgo2.Run(nodeToPoint, eps, topk, width)
  }*/

   if (OneStep) {
    val t = System.nanoTime()
    println("Single:::       eps:" + eps + "       topk:" + topk + "     cores:" + cores)
    matt.distrib.OnestepAlgo.Run(nodeToPoint, eps, topk, gridIndexer);
    println("Single:::       time:" + (System.nanoTime() - t) / 1000000000 + "s          eps:" + eps + "       topk:" + topk + "     cores:" + cores)
    println("-----------------------------------------------------------------------------------------------------------------------------")
    println("-----------------------------------------------------------------------------------------------------------------------------")
   }
   if (OneStepOptimized) {
    val t = System.nanoTime()
    println("Single:::           eps:" + eps + "       topk:" + topk + "     cores:" + cores)
    val nodeOptToPoint = inputData.rdd.flatMap(x => Generic.poiOptToKeyValue(x, geometryFactory, gridIndexer));
    matt.distrib.OnestepAlgoOptimized.Run(nodeOptToPoint, eps, decayConstant, topk, gridIndexer)
    println("SingleOpt:::       time:" + (System.nanoTime() - t) / 1000000000 + "s          eps:" + eps + "       topk:" + topk + "     cores:" + cores)
    println("-----------------------------------------------------------------------------------------------------------------------------")
    println("-----------------------------------------------------------------------------------------------------------------------------")
   }
  }
  spark.stop()
 };

}