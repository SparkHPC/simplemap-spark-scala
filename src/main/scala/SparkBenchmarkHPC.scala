/*
 * SimpleMap: Large array tests with RDDs using Breeze DenseMatrix[Double] 
 * for the block representation of array of (x, y, z) data. We use Array
 * of DenseMatrix[Double] to scale up the data size, since Java arrays (and
 * Scala by association) are presently limited to 32-bit size.
 *
 * Use --blocks parameter to multiply --block_size to get bigger arrays!
 */
package dataflows.spark

import java.io._

import blockperf._
import breeze.linalg._
import org.apache.spark.SparkContext
import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.util.Try

object SparkBenchmarkHPC {

  type BigMatrixXYZ = Array[DenseMatrix[Double]]

  val MEGA_MULTIPLIER = 1024 * 1024

  def main(args: Array[String]) {
    val config = parseCommandLine(args).getOrElse(Config())
    val experiment = Experiment("simplemap-spark-scala")
    val sc = new SparkContext()

    val (mapTime, _, a) = performance {
      val rdd = if (config.generate) {
        println("generated option")
        rddFromGenerate(sc, config)
      } else if (config.src.isDefined) {
        println("I/O option")
        createResultsDir(config.dst.getOrElse("."), "/results")
        rddFromBinaryFile(sc, config)
      } else {
        println("No option")
        sc.stop
        rddNOP(sc, config)
      }
      if (!config.lazyEval) {
        rdd.persist()
        val count = rdd.count() // force RDD eval
        println(s"RDD count / generate = $count")
      }
      rdd
    }

    val (shiftTime, _, b) = performance {
      val shiftResult = doShift(a)
      if (!config.lazyEval) {
        shiftResult.persist()
        val count = shiftResult.count() // force RDD eval
        println(s"RDD count / shift = $count")
      }
      shiftResult
    }

    val (avgTime, _, c) = performance {
      val averageResult = doAverage(b, config)
      if (!config.lazyEval) {
        averageResult.persist()
        val count = averageResult.count() // force RDD eval
        println(s"RDD count / average = $count")
      }
      averageResult
    }

    val (reduceTime, _, avgs) = performance {
      val avgs = c.reduce(_ + _)
      println(s"avg(x)=${avgs(0)} avg(y)=${avgs(1)} avg(z)=${avgs(2)}")
      avgs
    }

    val timings = Map(
      "map" -> mapTime.t / 1.0e9,
      "shift" -> shiftTime.t / 1.0e9,
      "average" -> avgTime.t / 1.0e9,
      "reduce" -> reduceTime.t / 1.0e9,
      "overall" -> (mapTime.t + shiftTime.t + avgTime.t + reduceTime.t) / 1.0e9
    )

    val report = Report(timings)
    if (config.jsonFilename.isDefined)
      writeJsonReport(experiment, config, report)
    if (config.xmlFilename.isDefined)
      writeXmlReport(experiment, config, report)
  }

  def writeJsonReport(exp: Experiment, config: Config, data: Report): Unit = {
    val results = exp.toJSON ~ config.toJSON ~ data.toJSON
    val file = new File(config.jsonFilename.get)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(pretty(results))
    bw.close()
  }

  def writeXmlReport(exp: Experiment, config: Config, data: Report): Unit = {
    val results = <results>
                    { exp.toXML }{ config.toXML }{ data.toXML }
                  </results>
    val pprinter = new scala.xml.PrettyPrinter(80, 2) // scalastyle:ignore
    val file = new File(config.xmlFilename.get)
    val bw = new BufferedWriter(new FileWriter(file))
    println("Wrote to XML file " + config.xmlFilename.get)
    bw.write(pprinter.format(results)) // scalastyle:ignore
    bw.close()
  }

  def nanoTime[R](block: => R): (Double, R) = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    (t1 - t0, result)
  }

  def parseCommandLine(args: Array[String]): Option[Config] = {
    val parser = new scopt.OptionParser[Config]("scopt") {
      head("simplemap-spark-scala", "0.1.x")
      opt[String]('s', "src") action { (x, c) =>
        c.copy(src = Some(x))
      } text ("s/src is a String property")
      opt[Unit]('g', "generate") action { (_, c) =>
        c.copy(generate = true)
      } text ("g/generate is a Boolean property")
      opt[Unit]('l', "lazy") action { (_, c) =>
        c.copy(lazyEval = true)
      } text ("l/lazy is as Boolean property (turn off caching)")
      opt[String]('d', "dst") action { (x, c) =>
        c.copy(dst = Some(x))
      } text ("d/dst is a String property")
      opt[Int]('b', "blocks") action { (x, c) =>
        c.copy(blocks = x)
      } text ("b/blocks is a int property")
      opt[Int]('m', "multiplier") action { (x, c) =>
        c.copy(multiplier = x)
      } text (s"m/multiplier is an int property (default = $MEGA_MULTIPLIER)")
      opt[Int]('k', "block_size") action { (x, c) =>
        c.copy(blockSize = x)
      } text (s"s/blockSize is an int property (number of 3D float vectors x $MEGA_MULTIPLIER)")
      opt[Int]('n', "nodes") action { (x, c) =>
        c.copy(nodes = x)
      } text ("n/nodes is an int property")
      opt[Int]('p', "nparts") action { (x, c) =>
        c.copy(nparts = x)
      } text ("p/nparts is an int property")
      opt[Int]('c', "cores") action { (x, c) =>
        c.copy(cores = x)
      } text ("c/cores is an int property (default to 12 for dual-hexcore on Cooley)")
      opt[String]('j', "json") action { (x, c) =>
        c.copy(jsonFilename = Some(x))
      } text (s"json <filename>is where to write JSON reports")
      opt[String]('x', "xml") action { (x, c) =>
        c.copy(xmlFilename = Some(x))
      } text (s"xml <filename> is where to write XML reports")
      opt[Unit]("functional") action { (_, c) =>
        c.copy(functional = true)
      } text ("functional is a boolean property (select 100% pure functional average using map and UFunc)")
      opt[Unit]("hybrid") action { (_, c) =>
        c.copy(hybrid = true)
      } text ("hybrid is a boolean property (use imperative outer loop and Breeze UFunc")
      opt[Unit]("imperative") action { (_, c) =>
        c.copy(imperative = true)
      } text ("imperative is a boolean property (use imperative code to do average without Breeze UFuncs")
      help("help") text ("prints this usage text")
    }
    parser.parse(args, Config())
  }

  def createResultsDir(dst: String, resultsDirName: String): Boolean = {
    val outdir = new File(dst, resultsDirName)
    Try {
      outdir.mkdirs()
    } getOrElse (false)
  }

  def rddFromBinaryFile(sc: SparkContext, config: Config): RDD[BigMatrixXYZ] = {
    val rdd = sc.binaryFiles(config.src.get)
    rdd.map(binFileInfo => parseVectors(binFileInfo))
  }

  def parseVectors(binFileInfo: (String, PortableDataStream)): BigMatrixXYZ = {
    val (path, bin) = binFileInfo
    val dis = bin.open()
    val iter = Iterator.continually(nextDoubleFromStream(dis))
    val scalaArray = iter.takeWhile(_.isDefined).map(_.get).toArray
    val dm = new DenseMatrix(scalaArray.length / 3, 3, scalaArray)
    Array(dm)
    // TODO: Need to fix read logic to allow for extremely large file-based arrays.
  }

  def nextDoubleFromStream(dis: DataInputStream): Option[Double] = {
    Try {
      dis.readDouble
    }.toOption
  }

  def rddNOP(sc: SparkContext, config: Config): RDD[BigMatrixXYZ] = {
    rddFromGenerate(sc, Config())
  }

  def rddFromGenerate(sc: SparkContext, config: Config): RDD[BigMatrixXYZ] = {
    val rdd = sc.parallelize(0 to config.blocks, config.nodes * config.cores * config.nparts)
    //val gen_block_count = (config.blockSize * 1E6 / 24).toInt // 24 bytes per vector
    rdd.map(item => generate(item, config.blockSize, config.multiplier))
  }

  // This is the RDD lambda
  def generate(id: Int, blockSize: Int, blockSizeMultiplier: Int): BigMatrixXYZ = {
    // Owing to Java array size limitation, we'll multiply array size by using an outer Array
    // to hold each block (as an inner DenseMatrix)
    val (deltat, _, array) = performance {
      Array.fill(blockSize)(generateBlock(id, blockSizeMultiplier))
    }
    val blockCount = blockSize * blockSizeMultiplier
    println(s"Array (block id=$id) of $blockCount float vectors, time = $deltat")
    array
  }

  // This is not
  def generateBlock(id: Int, blockSize: Int): DenseMatrix[Double] = {
    val randomDoubleGenerator = RandomDoubles(id, -1000, 1000)
    DenseMatrix.fill(blockSize, 3)(randomDoubleGenerator.next)
  }

  def doShift(a: RDD[BigMatrixXYZ]): RDD[BigMatrixXYZ] = {
    val shift = DenseVector(25.25, -12.125, 6.333)
    a.map(x => addVectorDisplacement(x, shift))
  }

  def doAverage(a: RDD[BigMatrixXYZ], config: Config): RDD[DenseVector[Double]] = {
    if (config.functional)
      a.map(x => averageOfVectorsUFunc(x))
    else if (config.hybrid)
      a.map(x => averageOfVectorsUFuncWhile(x))
    else if (config.imperative)
      a.map(x => averageOfVectorsNoUFuncWhile(x))
    else // default to functional
      a.map(x => averageOfVectorsUFunc(x))
  }

  def averageOfVectorsUFunc(data: BigMatrixXYZ): DenseVector[Double] = {
    // computes sum of each column (x, y, and z)
    // dividing by a dense vector gives avg(x), avg(y), avg(z)
    // we'll then reduce this to get the global average across partitions

    val (deltat, _, avg) = performance {
      val partialAverageIter = data map { matrix =>
        // Breeze 0.12
        //println(s"averageOfVectorsUFunc() inner loop matrix: rows = ${matrix.rows} cols = ${matrix.cols}")

        sum(matrix(::, *)).t map { element => element / matrix.rows.toDouble }

        // Breeze 0.11.2 (same as Spark Breeze version)
        //sum(matrix(::, *)).toDenseVector / DenseVector.fill(matrix.cols)(matrix.rows.toDouble)
      }
      partialAverageIter.reduce(_ + _)
    }
    val finalAvg = avg map { element => element / data.length.toDouble }
    println(s"averageOfVectorsUFunc() time = $deltat  data.length=${data.length}  inner matrix: rows = ${data(0).rows} cols = ${data(0).cols}")
    finalAvg
  }

  def averageOfVectorsUFuncWhile(data: BigMatrixXYZ): DenseVector[Double] = {
    // computes sum of each column (x, y, and z)
    // dividing by a dense vector gives avg(x), avg(y), avg(z)
    // we'll then reduce this to get the global average across partitions

    val (deltat, _, vectorSum) = performance {
      var i = 0;
      val vectorSum = DenseVector.fill(data(0).cols)(0.0)

      // This version is using an explicit while loop to ensure that no threads are used
      // over map()

      while (i < data.length) {
        val matrix = data(i)
        //println(s"averageOfVectorsUFuncWhile() inner loop matrix: rows = ${matrix.rows} cols = ${matrix.cols}")
        val innerAvg = sum(matrix(::, *)).t map { element => element / matrix.rows.toDouble }
        vectorSum += innerAvg
        i += 1
      }
      vectorSum
    }

    val finalAvg = vectorSum map { element => element / data.length.toDouble }
    println(s"averageOfVectorsUFuncWhile() time = $deltat  data.length=${data.length}  inner matrix: rows = ${data(0).rows} cols = ${data(0).cols}")
    finalAvg
  }

  def averageOfVectorsNoUFuncWhile(data: BigMatrixXYZ): DenseVector[Double] = {
    // computes sum of each column (x, y, and z)
    // dividing by a dense vector gives avg(x), avg(y), avg(z)
    // we'll then reduce this to get the global average across partitions

    val (deltat, _, vectorSum) = performance {
      var i = 0;
      val vectorSum = DenseVector.fill(data(0).cols)(0.0)

      // This version is using an explicit while loop to ensure that no threads are used
      // over map()

      while (i < data.length) {
        val matrix = data(i)
        //println(s"averageOfVectorsNoUFuncWhile() inner loop matrix: rows = ${matrix.rows} cols = ${matrix.cols}")
        vectorSum += doMatrixAverage(matrix)
        i += 1
      }
      vectorSum
    }
    val finalAvg = vectorSum map { element => element / data.length.toDouble }
    println(s"averageOfVectorsUFuncWhile() time = $deltat  data.length=${data.length}  inner matrix: rows = ${data(0).rows} cols = ${data(0).cols}")
    finalAvg
  }

  def doMatrixAverage(matrix: DenseMatrix[Double]): DenseVector[Double] = {
    var i = 0;
    var vectorSum = DenseVector.fill(matrix.cols)(0.0) // .t is so each iter is not unfairly penalized to do transform on sum
    while (i < matrix.rows) {
      //vectorSum += matrix(i, ::) // This slices a row of the matrix
      vectorSum += DenseVector(matrix(i, 0), matrix(i, 1), matrix(i, 2))
      i += 1
    }
    vectorSum map { element => element / matrix.rows.toDouble }
  }

  def addVectorDisplacement(data: BigMatrixXYZ, shift: DenseVector[Double]): BigMatrixXYZ = {
    require {
      data.length > 0 && data(0).cols == shift.length
    }
    data foreach { matrix =>
      for (i <- 0 until matrix.rows)
        matrix(i, ::) :+= Transpose(shift)
    }
    data
  }

  case class Experiment(name: String) {
    def toXML(): xml.Elem = <experiment id={ name }/>
    def toJSON(): org.json4s.JsonAST.JObject = ("experiment" -> ("id" -> name))
  }

  case class RandomDoubles(id: Int, low: Double, high: Double) {
    val seed = System.nanoTime() / (id + 1)
    val generator = new scala.util.Random(seed)

    def next(): Double = low + (high - low) * generator.nextDouble()
  }

  case class Report(timings: Map[String, Double]) {
    def toXML(): xml.Node = {
      <performance>
        { timings map { case (name, value) => <time id={ name.toString } t={ value.toString } unit="s"/> } }
      </performance>
    }

    def toJSON(): org.json4s.JsonAST.JObject = {
      val timeData = timings map { case (name, value) => JField(name, JString(value.toString)) }
      JField("performance", timeData.toList)
    }
  }

  // command-line parameters

  case class Config(
      src: Option[String] = None,
      dst: Option[String] = None,
      cores: Int = 12,
      generate: Boolean = false,
      lazyEval: Boolean = false,
      blocks: Int = 1,
      blockSize: Int = 1, // 1 MB
      multiplier: Int = MEGA_MULTIPLIER,
      nparts: Int = 1,
      size: Int = 1,
      nodes: Int = 1,
      jsonFilename: Option[String] = None,
      xmlFilename: Option[String] = None,
      functional: Boolean = false,
      hybrid: Boolean = false,
      imperative: Boolean = false
  ) {

    def toXML(): xml.Elem = {
      <args>
        <property key="src" value={ src.getOrElse("") }/>
        <property key="dst" value={ src.getOrElse("") }/>
        <property key="cores" value={ cores.toString }/>
        <property key="generate" value={ generate.toString }/>
        <property key="lazy" value={ lazyEval.toString }/>
        <property key="blocks" value={ blocks.toString }/>
        <property key="block_size" value={ blockSize.toString } unit="MB"/>
        <property key="multiplier" value={ multiplier.toString }/>
        <property key="nparts" value={ nparts.toString }/>
        <property key="size" value={ size.toString }/>
        <property key="nodes" value={ nodes.toString }/>
        <property key="json" value={ jsonFilename.getOrElse("") }/>
        <property key="xml" value={ xmlFilename.getOrElse("") }/>
        <property key="functional" value={ functional.toString }/>
        <property key="hybrid" value={ hybrid.toString }/>
        <property key="imperative" value={ imperative.toString }/>
      </args>
    }

    def toJSON(): org.json4s.JsonAST.JObject = {
      val properties = ("src" -> src.getOrElse("")) ~ ("dst" -> dst.getOrElse("")) ~ ("cores" -> cores.toString) ~
        ("generate" -> generate.toString) ~ ("lazy" -> lazyEval.toString) ~
        ("blocks" -> blocks.toString) ~ ("block_size" -> blockSize.toString) ~ ("multiplier" -> multiplier.toString) ~
        ("block_size_unit" -> "MB") ~
        ("nparts" -> nparts.toString) ~ ("size" -> size.toString) ~ ("nodes" -> nodes.toString) ~
        ("json" -> jsonFilename.getOrElse("")) ~ ("xml" -> xmlFilename.getOrElse("")) ~
        ("functional" -> functional.toString) ~ ("hybrid" -> hybrid.toString) ~ ("imperative" -> imperative.toString)

      ("args" -> properties)
    }

  }

}
