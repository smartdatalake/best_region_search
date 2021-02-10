package SDL.distrib

import SDL.ca.BCAIndexProgressiveOneRoundRedHybrid
import SDL.definitions.GridIndexer
import SDL.score.{OneStepResult, ScoreFunctionTotalScore}
import SDL.{DependencyGraph, POI, SpatialObject}
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.collection.immutable.HashSet
import scala.collection.mutable.ListBuffer

object OnestepAlgoReduceHybridOpt {

  def oneStepAlgo(input: (Int, Iterable[POI]), eps: Double, topk: Int, previous: ListBuffer[SpatialObject], gridIndexer: GridIndexer): (Int, OneStepResult) = {
    val pois: java.util.List[POI] = ListBuffer(input._2.toList: _*)
    val scoreFunction = new ScoreFunctionTotalScore[POI]()
    val bcaFinder = new BCAIndexProgressiveOneRoundRedHybrid(true, gridIndexer)
    (input._1, bcaFinder.findBestCatchmentAreas(pois, input._1, eps, topk, scoreFunction, previous))
  }

  def Run(nodeToPoint: RDD[(Int, POI)], eps: Double, topk: Int, gridIndexer: GridIndexer, base: Int, Kprime: Int) {
    var Ans = ListBuffer[SpatialObject]()
    var t = 0.0
    //  var mapped = new mutable.HashMap[Int, Int]
    //  var used = new mutable.HashMap[Int, Int]
    var round = 1
    var rdds: Array[RDD[(Int, OneStepResult)]] = new Array[RDD[(Int, OneStepResult)]](base * roundUp(math.log(gridIndexer.width) / math.log(base)) + 1)
    t = System.nanoTime()
    rdds(0) = nodeToPoint.groupByKey().map(x => oneStepAlgo(x, eps, Kprime, Ans, gridIndexer))
    //rdds(0).cache()
    //  println(rdds(0).map(x=>x._2.countUnsafe+x._2.countSafe).sum())
    //return
    //   println("round:::"+round+ rdds(0).count()+"   time::::" +(System.nanoTime() - t) / 1000000000)

    var lvl = 0
    while (Ans.size < topk) {
      lvl = 1;
      // println(rdds(0).count())
      //  println(roundUp(math.log(gridIndexer.width) / math.log(base)))
      while (lvl <= roundUp(math.log(gridIndexer.width) / math.log(base))) {
        var t = System.nanoTime()
        rdds(lvl) = rdds(lvl - 1).map(x => mapper(x._1, x._2, gridIndexer, lvl, base: Int)).groupByKey().map(x => reducer(x._1, x._2, gridIndexer, lvl, base, topk, Ans))
        rdds(lvl).cache()
        println(lvl + ":::" + rdds(lvl).count())
        // rdds(lvl-1)=null
        lvl += 1
      }
      var t = System.nanoTime()
      val roundResults = rdds(lvl - 1).map(x => x._2).collect().toList.get(0).spatialObjects
      //  System.out.println("dependency size:::"+(rdds(lvl - 1).map(x => x._2).collect().toList.get(0).countUnsafe))
      Ans.addAll(roundResults)
      //   println("collecttime::::" +(System.nanoTime() - t) / 1000000000)

      //println(Ans.sortBy(_.getScore).reverse)
      var topKIndex: HashSet[Int] = HashSet();
      for (spatialObject <- roundResults) {
        topKIndex.+=(spatialObject.getPart);
        topKIndex.+=(spatialObject.getPart - 1);
        topKIndex.+=(spatialObject.getPart + 1);
        topKIndex.+=(spatialObject.getPart - gridIndexer.width);
        topKIndex.+=(spatialObject.getPart + gridIndexer.width);
      }
      /*gridIndexer.getNodeIndex(spatialObject.getGeometry.getCoordinates.toList(1).x,spatialObject.getGeometry.getCoordinates.toList(1).y)
  .foreach(x =>topKIndex+=x._2*gridIndexer.width+x._1+1)*/
      //   println(topKIndex)
      round += 1

      t = System.nanoTime()
      val partialRoundRDD = nodeToPoint.groupByKey().filter(x => topKIndex.contains(x._1)).map(x => oneStepAlgo(x, eps, Kprime, Ans, gridIndexer))
      //  println("filterround:::"+round+"   time::::" +(System.nanoTime() - t) / 1000000000+"count:::::"+ partialRoundRDD.count())
      t = System.nanoTime()
      rdds(0) = rdds(0).filter(x => !topKIndex.contains(x._1)).union(partialRoundRDD)
      rdds(0).cache()
      //   println("unionround:::"+round+"   time::::" +(System.nanoTime() - t) / 1000000000+"count:::::"+ rdds(0).count())
      //  System.out.println(rdds(0).count())

      //rdds= new Array[RDD[(Int, OneStepResult)]](base * roundUp(math.log(gridIndexer.width) / math.log(base)) + 1)

    }
    //imple selecting query partition all partition that have less than k' safe region
    //print out frequency of querying partition and icrease for 3rd 4rth round
    Ans = Ans.sortBy(_.getScore).reverse
    System.err.println("SingleHybridOpt," + topk + " eps," + eps)
    for (i <- 0 to (topk - 1)) {
      System.err.println((i + 1) + ":" + Ans.get(i).getId + "     " + Ans.get(i).getScore);

    }
    //  Ans.sortBy(_.getScore).reverse.foreach(x => System.err.println(x.getId + ":::::::" + x.getScore))
  }

  def mapper(index: Int, result: OneStepResult, gridIndexer: GridIndexer, lvl: Int, base: Int): (Int, OneStepResult) = {
    val (nodeI, nodeJ) = ((index - 1) % width(lvl - 1, base: Int, gridIndexer), ((index - 1) / width(lvl - 1, base: Int, gridIndexer).asInstanceOf[Double]).toInt)
    ((nodeI / base).toInt + (nodeJ / base).toInt * width(lvl, base: Int, gridIndexer) + 1, result)
  }

  def reducer(index: Int, results: Iterable[OneStepResult], gridIndexer: GridIndexer, lvl: Int, base: Int, topK: Int, Ans: ListBuffer[SpatialObject]): (Int, OneStepResult) = {
    var preSafe = 0
    var preUnsafe = 0
    val dependencyGraph = new DependencyGraph(gridIndexer)
    results.foreach(x => {
      preSafe += x.countSafe
      preUnsafe += x.countUnsafe
    })
    val I = ((index - 1) % width(lvl, base: Int, gridIndexer))
    val J = (((index - 1) / (width(lvl, base: Int, gridIndexer)).asInstanceOf[Double]).toInt)
    val cornerALong = I * math.pow(base, lvl).toInt * gridIndexer.gridSizePerCell
    val cornerALat = J * math.pow(base, lvl).toInt * gridIndexer.gridSizePerCell
    val cornerBLong = cornerALong + math.pow(base, lvl).toInt * gridIndexer.gridSizePerCell - 1
    val cornerBLat = cornerALat + math.pow(base, lvl).toInt * gridIndexer.gridSizePerCell - 1
    var maxMin = 0.0
    var minlocal = 200000.0
    results.foreach(x => {
      if (maxMin < x.minSafe)
        maxMin = x.minSafe
    })
    var candidates = new ListBuffer[SpatialObject]
    results.foreach(x => candidates.addAll(x.spatialObjects))
    candidates = candidates.sortBy(_.getScore).reverse
    var pos = 0
    var unsafe = 0
    while (dependencyGraph.safeRegionCnt < topK && pos < candidates.size && candidates.get(pos).getScore >= maxMin) {
      val instance = candidates.get(pos)
      //  if ( !Generic.intersectsList(instance,Ans)) {
      val con = dependencyGraph.overlapCon(instance);
      val (cellI, cellJ) = gridIndexer.getCellIndex(instance.getGeometry.getCoordinates.toList(1).x.toFloat
        , instance.getGeometry.getCoordinates.toList(1).y.toFloat)
      if (con == 0 && !(cellI == cornerALong || cellI == cornerBLong || cellJ == cornerALat || cellJ == cornerBLat))
        dependencyGraph.addSafeRegion(instance)
      else if (con == 1) {
        val a = 0
      }
      else if (con == 2 || (cellI == cornerALong || cellI == cornerBLong || cellJ == cornerALat || cellJ == cornerBLat)) {
        dependencyGraph.addUnsafeRegion(instance);
        unsafe += 1
        if (dependencyGraph.IsDependencyIncluded(instance)) {
          // Do not add Safe
          // Do not add M
          var a = 0
        } else {
          dependencyGraph.increaseSafeCNT();
          dependencyGraph.addM(instance);
        }
        //    }
      }
      pos += 1
    }
    (index, new OneStepResult(preSafe, preUnsafe, index, maxMin.toInt, dependencyGraph.getFinalResult()))
  }

  def roundUp(d: Double) = math.ceil(d).toInt

  def width(lvl: Int, base: Int, gridIndexer: GridIndexer): Int = {
    var width = gridIndexer.width
    for (i <- 1 to lvl)
      width = roundUp(width / base.asInstanceOf[Double])
    return width
  }

  /*
  1:9.99829845:53.5948001     8.0
  2:9.951635750000001:52.154821     10.0
  3:9.9349632:51.53452995     7.0
  4:9.9304199:49.79635345     10.0
  5:9.883957:45.59845825     5.0
  6:9.8488758:55.86298445     5.0
  7:9.837147300000002:54.4708188     10.0
  8:9.789265499999999:48.1670431     7.0
  9:9.6769102:50.553815799999995     9.0
  10:9.53646405:55.70837135     5.0
  11:9.42364745:53.7871138     8.0
  12:9.37541225:45.70575475     6.0
  13:9.186170350000001:45.55172705     6.0
  14:9.186131:45.55047295     7.0
  15:8.965811500000001:54.61818445     5.0
  16:8.96396235:54.617874150000006     5.0
  17:8.91982585:50.1324519     10.0
  18:8.9190881:50.1330522     13.0
  19:8.8514354:51.696589849999995     6.0
  20:8.8139051:53.0816989     8.0
  21:8.74742805:45.83875605     7.0
  22:8.74564435:52.08524465     7.0
  23:8.6749951:50.5866412     6.0
  24:8.658263900000001:51.9594447     7.0
  25:8.543828300000001:47.3747507     9.0
  26:8.543564:47.37389975     14.0
  27:8.460405399999999:48.05844195     16.0
  28:8.46008535:48.05921185     15.0
  29:8.34232675:47.3517486     9.0
  30:8.21392465:53.143090650000005     7.0
  31:8.068997:52.252135249999995     9.0
  32:7.9465428:48.468912     6.0
  33:7.8816351000000004:46.2931498     6.0
  34:7.86400395:51.331293650000006     8.0
  35:7.84124035:47.9965756     10.0
  36:7.76817795:50.0595557     12.0
  37:7.74537585:48.60207335     7.0
  38:7.5718390499999995:50.42503115     9.0
  39:7.3393365500000005:52.1478162     11.0
  40:7.2279932:46.09620375     7.0
  41:7.21661965:51.47688305     8.0
  42:7.1968685:53.3692592     7.0
  43:7.1122112:49.2782688     7.0
  44:7.0280764:49.2002048     6.0
  45:7.02555375:51.42090955     12.0
  46:6.9437906:50.9384643     12.0
  47:6.8408709:46.46309025     7.0
  48:6.74950965:49.31733215     6.0
  49:6.6284603:46.516646800000004     8.0
  50:6.35619975:46.903128300000006     15.0
  51:6.353238:46.904325400000005     14.0
  52:5.930960600000001:45.32387895     6.0
  53:5.9221323:52.96061975     6.0
  54:5.7337456499999995:58.9707048     8.0
  55:5.7332096:58.970914449999995     6.0
  56:5.7237088499999995:45.186828250000005     6.0
  57:5.7188564500000005:45.186049600000004     10.0
  58:5.30574645:51.68808935     12.0
  59:5.3046367:51.688708399999996     11.0
  60:5.29827555:43.62090345     8.0
  61:5.0594559:52.639630600000004     8.0
  62:44.997757050000004:53.1758018     8.0
  63:4.88841195:52.3524148     8.0
  64:4.84781245:45.759924350000006     8.0
  65:4.83476795:45.7572059     14.0
  66:4.8062873:43.94933965     7.0
  67:4.5024973500000005:52.1955172     6.0
  68:4.4770837:51.0263592     6.0
  69:4.4702392500000006:52.185663649999995     6.0
  70:4.4342724:51.2015511     6.0
  71:37.623916699999995:55.63351     5.0
  72:34.84709705:38.71871405     8.0
  73:33.29596035:34.77206565     10.0
  74:32.8604066:39.902952850000005     8.0
  75:32.8547459:39.8851124     8.0
  76:32.4076955:34.755328199999994     5.0
  77:30.31121975:59.9665634     5.0
  78:3.1357431499999997:39.84089795     6.0
  79:3.1340134500000003:39.84208435     8.0
  80:29.8614156:36.1900447     5.0
  81:29.41500825:36.2650704     8.0
  82:28.86214855:46.9822405     5.0
  83:28.7738542:46.949584     6.0
  84:28.05559435:45.4351857     7.0
  85:28.0553333:45.434415650000005     5.0
  86:27.65911645:63.07332005     5.0
  87:27.65836555:63.074537649999996     6.0
  88:27.591538999999997:47.1479324     6.0
  89:27.5887129:47.15289045     6.0
  90:26.942718499999998:37.690032599999995     6.0
  91:26.93961755:37.690291099999996     7.0
  92:26.914319050000003:37.6912989     5.0
  93:26.436148250000002:46.20580095     5.0
  94:26.008439850000002:61.5790693     5.0
  95:25.77172585:62.2278381     6.0
  96:25.25773995:62.7063677     5.0
  97:25.0767569:60.209817650000005     8.0
  98:24.9395562:60.17192125     15.0
  99:24.8576373:60.6294088     7.0
  100:24.856327399999998:60.627953500000004     11.0
  101:24.79443625:46.217806800000005     5.0
  102:24.7238066:36.9737571     5.0
  103:24.5962598:56.8216111     7.0
  104:24.5838299:46.53483045     7.0
  105:24.5138122:37.14273465     10.0
  106:23.6958026:52.0875785     5.0
  107:23.131827649999998:63.838648899999995     6.0
  108:22.9492148:40.6468322     9.0
  109:22.9006306:45.88239235     5.0
  110:22.638847300000002:41.43901285     6.0
  111:22.56967555:36.75634235     5.0
  112:22.336312300000003:42.20568865     6.0
  113:21.795216099999998:61.4842377     5.0
  114:21.6145551:63.096571600000004     5.0
  115:21.613679599999998:63.095625999999996     5.0
  116:21.31652485:46.17238545     6.0
  117:21.25965625:48.721384099999995     5.0
  118:21.2484071:41.3680939     6.0
  119:21.241336099999998:48.9959717     5.0
  120:21.2221756:45.77460825     6.0
  121:21.012676550000002:52.25001305000001     9.0
  122:21.01097815:52.249708299999995     11.0
  123:2.3725878:48.847034949999994     7.0
  124:2.3719069499999996:48.846019999999996     8.0
  125:2.02899005:49.0109839     8.0
  126:19.94030625:50.063290499999994     11.0
  127:19.93678735:50.0631595     10.0
  128:19.74944695:53.5036974     5.0
  129:19.1120236:45.771794650000004     7.0
  130:19.0507764:47.501769100000004     6.0
  131:18.96198365:47.563761650000004     5.0
  132:18.583130699999998:48.779272500000005     5.0
  133:18.07157085:59.34613415     7.0
  134:18.0434854:59.32940895     6.0
  135:17.83457325:48.75158465     6.0
  136:17.6147385:42.9266133     6.0
  137:17.10422825:48.14050475     6.0
  138:17.032442099999997:51.1122772     5.0
  139:16.93424925:52.4086187     8.0
  140:16.9329373:52.406337449999995     8.0
  141:16.87116535:41.119756949999996     6.0
  142:16.5519806:41.21288735     6.0
  143:16.408099149999998:48.2763271     11.0
  144:16.38842365:48.20418465     10.0
  145:16.15454715:47.4569973     5.0
  146:15.9962904:45.8146737     5.0
  147:15.59017205:49.3951401     5.0
  148:15.5892151:49.397404800000004     5.0
  149:15.4420015:47.06737555     7.0
  150:15.44173575:47.066801100000006     7.0
  151:15.36858565:48.4624491     6.0
  152:15.1044701:37.51991245     5.0
  153:15.0571269:37.508281999999994     5.0
  154:14.99088875:51.156361950000004     5.0
  155:14.678274049999999:51.102426699999995     5.0
  156:14.63584855:52.15241845     5.0
  157:14.6127988:48.416941800000004     5.0
  158:14.57197125:52.253849450000004     6.0
  159:14.5601219:53.4238857     6.0
  160:14.54915245:53.425690349999996     5.0
  161:14.479690900000001:48.21379345     7.0
  162:14.479030250000001:48.21376825     7.0
  163:14.41831795:50.0743202     6.0
  164:14.3630091:50.0836313     6.0
  165:14.309241:46.626148650000005     6.0
  166:14.306426250000001:46.62529615     7.0
  167:14.2596147:36.0707466     5.0
  168:14.21750065:36.0307747     6.0
  169:14.20893785:48.0349304     6.0
  170:14.16678205:42.5028122     5.0
  171:14.15418665:42.51280035     7.0
  172:13.94409155:50.96288655     6.0
  173:13.64825505:50.9988666     6.0
  174:13.41805055:54.080807050000004     5.0
  175:13.417935:52.49326215     6.0
  176:13.404979449999999:52.53422645     7.0
  177:13.35288165:38.1256546     5.0
  178:13.31198485:45.9058513     5.0
  179:13.308940549999999:45.9057578     10.0
  180:13.2534365:53.55721205     6.0
  181:13.20291405:52.53781405     6.0
  182:13.145647400000001:58.50807425     5.0
  183:13.06818745:47.781283     6.0
  184:13.0053139:50.58083365     6.0
  185:12.8443593:49.30921015     7.0
  186:12.63334895:44.00221845     5.0
  187:12.6217368:45.497706699999995     5.0
  188:12.57207425:48.882124250000004     10.0
  189:12.5696044:48.88184325     11.0
  190:12.516843699999999:55.66271665     10.0
  191:12.51615075:55.663077099999995     7.0
  192:12.4256215:51.3570465     6.0
  193:12.41886785:49.46011195     5.0
  194:12.32117575:47.76481205     7.0
  195:12.2228133:51.8470979     7.0
  196:12.135860749999999:54.0857173     7.0
  197:12.0146359:47.673683999999994     7.0
  198:12.00660205:47.862358549999996     8.0
  199:11.9732821:57.7094793     5.0
  200:11.6359335:48.17310395     11.0
  201:11.624487599999998:52.1313607     7.0
  202:11.59361505:52.1075106     8.0
  203:11.577534:49.943480300000004     7.0
  204:11.57472315:48.156838449999995     11.0
  205:11.421244399999999:43.23222805     5.0
  206:11.414566449999999:53.62884235     9.0
  207:11.41248485:53.6283544     10.0
  208:11.3750995:44.646426500000004     9.0
  209:11.3068846:44.71373765     6.0
  210:11.2600853:45.740839     6.0
  211:11.1211109:46.06774555     6.0
  212:10.9649073:50.2592139     11.0
  213:10.919246950000002:50.683388300000004     8.0
  214:10.9126147:50.684024949999994     9.0
  215:10.8445552:59.754492049999996     11.0
  216:10.84107215:45.9078661     6.0
  217:10.78577005:51.833963749999995     10.0
  218:10.69930095:47.569842550000004     5.0
  219:10.6932418:45.43866025     9.0
  220:10.18314075:47.25799875     5.0
  221:10.162222400000001:49.7391456     11.0
  222:10.131222900000001:54.31581835     8.0
  223:10.0948949:48.840598299999996     7.0
  224:10.04572165:53.5346879     34.0
  225:1.75166265:52.4772561     9.0
  226:1.60874855:41.57325395     6.0
  227:1.6086251:41.5738616     6.0
  228:1.15498525:52.05540845     12.0
  229:1.08210815:51.276626050000004     10.0
  230:1.0814075:51.276336799999996     8.0
  231:0.33993125:45.247996     6.0
  232:0.14360030000000001:52.197921     12.0
  233:0.0759467:43.23162965     10.0
  234:0.07447205:43.2324263     8.0
  235:-9.34627995:52.93278885     6.0
  236:-8.53194645:52.910374250000004     7.0
  237:-8.53143045:52.911116699999994     6.0
  238:-8.17086025:43.40673365     9.0
  239:-6.67733215:53.6633018     9.0
  240:-6.5928927:53.38125615     7.0
  241:-6.38768595:58.2082504     7.0
  242:-6.31629985:49.9146656     7.0
  243:-6.2653945:53.3163238     9.0
  244:-6.26415045:53.34335145     8.0
  245:-6.1467314:52.7992305     6.0
  246:-4.98940285:36.48490305     7.0
  247:-4.48757305:55.5920641     7.0
  248:-4.45929525:57.4835172     6.0
  249:-4.27588335:53.1402121     6.0
  250:-4.22519475:57.480758699999996     8.0
  251:-4.17392085:55.76057605     7.0
  252:-4.1432222:50.951793050000006     6.0
  253:-4.130369699999999:52.926449500000004     6.0
  254:-4.1078980000000005:52.8591972     6.0
  255:-3.6905072:40.48590575     7.0
  256:-3.6899038:40.48579745     7.0
  257:-3.61128325:55.071151099999994     8.0
  258:-3.56647385:50.4346755     9.0
  259:-3.52411315:50.46125535     6.0
  260:-3.3604301999999997:55.9469839     10.0
  261:-3.19124695:55.95544945     9.0
  262:-3.1710137:51.4918408     8.0
  263:-3.1317083500000003:51.5089832     9.0
  264:-3.026981:53.3614941     11.0
  265:-2.93224365:54.89429355     8.0
  266:-2.9255166499999996:43.2611641     6.0
  267:-2.8922404:53.394658750000005     12.0
  268:-2.6723495:42.8469582     8.0
  269:-2.6694886:42.862474750000004     7.0
  270:-2.5927074:51.45918375     10.0
  271:-2.58596885:56.5591533     9.0
  272:-2.5534105499999997:48.58996445     7.0
  273:-2.5186368:52.70100675     12.0
  274:-2.1843461499999997:51.26091585     10.0
  275:-1.9753965500000001:43.3050499     6.0
  276:-1.8979675500000002:43.31273285     7.0
  277:-1.8253906500000001:52.68135575     16.0
  278:-1.7867982:53.64530775     13.0
  279:-1.6205205:48.08771215     10.0
  280:-1.6149084:54.97667385     11.0
  281:-1.58208905:54.7773807     11.0
  282:-1.55532145:47.2148304     8.0
  283:-1.55330795:47.21538125     11.0
  284:-1.5358866:53.79740245     15.0
  285:-1.49798645:53.68569565     16.0
  286:-1.4665504:52.5244742     10.0
  287:-1.3851532500000001:54.902487550000004     10.0
  288:-1.33590915:54.7593737     8.0
  289:-1.1388547999999998:53.52436725     19.0
  290:-0.88989685:48.8387859     8.0
  291:-0.83896345:48.9369633     8.0
  292:-0.64174275:52.9096568     11.0
  293:-0.5198273499999999:38.3944831     8.0
  294:-0.41677365:38.37065215     6.0
  295:-0.3802141:39.47283215     6.0
  296:-0.37464655:51.6075723     13.0
  297:-0.3070629:51.43077665     14.0
  298:-0.24219605:52.57320385     14.0
  299:-0.21696185:51.4611236     18.0
  300:-0.1585784:51.4917886     17.0
  */
}