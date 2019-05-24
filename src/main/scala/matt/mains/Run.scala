package matt.mains

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{lower, max, min, when}

import matt.definitions.GridIndexer
import org.apache.spark.{SparkConf, SparkContext};
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;
import matt.definitions.TableDefs
import matt.definitions.Generic
import org.apache.log4j.Logger
import org.apache.log4j.Level

object Run {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val con = new SparkConf().setAppName("simple App").setMaster("local[2]")
    val sc = new SparkContext(con)
    val spark = SparkSession
      .builder
      .appName("Simple Application")
      .master("local[*]")
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

    val poiInputFile2 = "/home/hamid/4.csv";
    val poiInputFile = "/home/hamid/temp.csv";

    val eps = 0.001
    // choose number of expected results
    val topk = 10
    val decayConstant = 0.5

    val inputData2 = spark.read.format("csv").option("header", "true").option("delimiter", ";").schema(TableDefs.customSchema2).load(poiInputFile).drop();
    val inputData = inputData2.filter(x => (x.getAs[Float]("longtitude") != null && x.getAs[Float]("latitude") != null))
    inputData.cache();
    // set number of cores
    val cores = 16
    val width = scala.math.sqrt(cores).toInt;

    val minLong = inputData.select("longtitude").reduce((x, y) => if (x.getAs[Float]("longtitude") < y.getAs[Float]("longtitude")) x else y).getAs[Float](0)

    val maxLong = inputData.select("longtitude").reduce((x, y) => if (x.getAs[Float]("longtitude") > y.getAs[Float]("longtitude")) x else y).getAs[Float](0)
    val minLat = inputData.select("latitude").reduce((x, y) => if (x.getAs[Float]("latitude") < y.getAs[Float]("latitude")) x else y).getAs[Float](0)
    val maxLat = inputData.select("latitude").reduce((x, y) => if (x.getAs[Float]("latitude") > y.getAs[Float]("latitude")) x else y).getAs[Float](0)
    // choose width of grid (the number of cores required is N squared)
    val minmaxLongArra = inputData.agg(min("longtitude"), max("longtitude")).rdd.map(r => r(0)).collect()
    val minmaxLatArra = inputData.agg(min("latitude"), max("latitude")).rdd.map(r => r(0)).collect()
    println(2)

    val minmaxLong =  (minLong-0.00001.asInstanceOf[Float], maxLong+0.0001.asInstanceOf[Float]);
    println("\n\nminmaxLONG: " + minmaxLong + "\n\n");
    val minmaxLat = (minLat-0.00001.asInstanceOf[Float], maxLat+0.00001.asInstanceOf[Float]);
    println("\n\nminmaxLat: " + minmaxLat + "\n\n");
    val gridIndexer= new GridIndexer(width,eps,minmaxLong,minmaxLat)
    // find to which node does each point belongs to : (NodeNo,Row)
    val geometryFactory = new GeometryFactory(new PrecisionModel(), 4326);
    val nodeToPoint = inputData.rdd.flatMap(x => Generic.poiToKeyValue(x, width, minmaxLong, minmaxLat,eps.asInstanceOf[Float], geometryFactory,gridIndexer));
    nodeToPoint.groupByKey().foreach(x=> println(x._2.size))
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