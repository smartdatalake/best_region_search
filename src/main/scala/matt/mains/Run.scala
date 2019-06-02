package matt.mains

import java.net.URI

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
      .config("spark.executor.memory", "7g")
      .config("spark.driver.maxResultSize","3g")
      .config("spark.driver.memory", "7g")
      .config("spark.memory.offHeap.enabled",true)
      .config("spark.memory.offHeap.size","7g")
      .getOrCreate()
   val hadoopConfig: Configuration = spark.sparkContext.hadoopConfiguration
   hadoopConfig.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
   hadoopConfig.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
    val poiInputFile = "/home/hamid/5.csv";
    val poiInputFile2 = "/home/hamid/temp.csv";
    val poiInputFile3 = "/home/hamid/input.csv";

    val eps = 0.001
    // choose number of expected results
    val topk = 10
    val decayConstant = 0.5
    val cores = 16*16
    val width = scala.math.sqrt(cores).toInt
    //////end Param & config
    //////////////////////////////////////

    //////Read and split CSV coordination to (nodeNumber, POI) (assign poi to each worker)
    ///////////////////////////////////////////////////////////////

    //val inputData = spark.read.format("csv").option("header", "true").option("delimiter", ";").schema(TableDefs.customSchema2).load("hdfs:///input.csv").drop().filter(x => (x.getAs[Double]("longtitude") != null && x.getAs[Double]("latitude") != null));;
    val inputData = spark.read.format("csv").option("header", "true").option("delimiter", ";").schema(TableDefs.customSchema2).load(poiInputFile).drop().filter(x => (x.getAs[Double]("longtitude") != null && x.getAs[Double]("latitude") != null));



    val minLong = inputData.select("longtitude").reduce((x, y) => if (x.getAs[Double]("longtitude") < y.getAs[Double]("longtitude")) x else y).getAs[Double](0)

    val maxLong = inputData.select("longtitude").reduce((x, y) => if (x.getAs[Double]("longtitude") > y.getAs[Double]("longtitude")) x else y).getAs[Double](0)
    val minLat = inputData.select("latitude").reduce((x, y) => if (x.getAs[Double]("latitude") < y.getAs[Double]("latitude")) x else y).getAs[Double](0)
    val maxLat = inputData.select("latitude").reduce((x, y) => if (x.getAs[Double]("latitude") > y.getAs[Double]("latitude")) x else y).getAs[Double](0)


    val minmaxLong =  (minLong-0.00001, maxLong+0.0001);
    println("\n\nminmaxLONG: " + minmaxLong + "\n\n");
    val minmaxLat = (minLat-0.00001, maxLat+0.00001);
    println("\n\nminmaxLat: " + minmaxLat + "\n\n");


    val gridIndexer= new GridIndexer(width,eps,minmaxLong,minmaxLat)
    // find to which node does each point belongs to : (NodeNo,Row)
    val geometryFactory = new GeometryFactory(new PrecisionModel(), 4326);
    val nodeToPoint = inputData.rdd.flatMap(x => Generic.poiToKeyValue(x, width, minmaxLong, minmaxLat,eps.asInstanceOf[Double], geometryFactory,gridIndexer)).cache();

    ////////End Read & split data poi to each worker
    //////////////////////////////////////////////////////////////////////////////



    ///////Start
    //////////////////////////////
    val Nstep = true;
    val Nstep2= true
    val OneStep = false;

    if (Nstep) {
       matt.distrib.NstepAlgo.Run(nodeToPoint, eps, topk);
    }

    if(Nstep2){
      matt.distrib.NstepAlgo2.Run(nodeToPoint,eps,topk,width)
    }

    if (OneStep) {
      matt.distrib.OnestepAlgo.Run(nodeToPoint, eps, decayConstant, topk,gridIndexer);
    }

    spark.stop()
  };

}