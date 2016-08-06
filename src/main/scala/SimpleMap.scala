/*
 * SimpleMap: Large array tests with RDDs using Breeze DenseMatrix[Double] 
 * for the block representation of array of (x, y, z) data. We use Array
 * of DenseMatrix[Double] to scale up the data size, since Java arrays (and
 * Scala by association) are presently limited to 32-bit size.
 *
 * Use --blocks parameter to multiply --block_size to get bigger arrays!
 */

package edu.luc.cs

import org.apache.spark.SparkContext
import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD
import scala.util.{ Try, Success, Failure }
import java.io._
import breeze.linalg._

import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._

object SimpleMap {

  type BigMatrixXYZ = Array[DenseMatrix[Double]]

  val MAX_DENSE_MATRIX_ROWS = Int.MaxValue / 4

  case class Experiment(name: String) {
    def toXML(): xml.Elem = <experiment id={ name }/>
    def toJSON(): org.json4s.JsonAST.JObject = ("experiment" -> ("id" -> name))
  }

  def main(args: Array[String]) {
    val config = parseCommandLine(args).getOrElse(Config())
    val experiment = Experiment("simplemap-spark-scala")
    val sc = new SparkContext()

    val (mapTime, a) = nanoTime {
      if (config.generate) {
        rddFromGenerate(sc, config)
      } else if (config.src.isDefined) {
        createResultsDir(config.dst.getOrElse("."), "/results")
        rddFromBinaryFile(sc, config)
      } else {
        sc.stop
        rddNOP(sc, config)
      }
    }

    val (shiftTime, b) = nanoTime {
      doShift(a)
    }

    val report = Report(mapTime, shiftTime)
    if (config.jsonFilename.isDefined)
      writeJsonReport(experiment, config, report)
    if (config.xmlFilename.isDefined)
      writeXmlReport(experiment, config, report)
  }

  def writeJsonReport(exp: Experiment, config: Config, data: Report): Unit = {
    val results = "results" -> (exp.toJSON ~ config.toJSON ~ data.toJSON)
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
      opt[String]('d', "dst") action { (x, c) =>
        c.copy(dst = Some(x))
      } text ("d/dst is a String property")
      opt[Int]('b', "blocks") action { (x, c) =>
        c.copy(blocks = x)
      } text ("b/blocks is a int property")
      opt[Int]('s', "block_size") action { (x, c) =>
        c.copy(blockSize = math.min(x, MAX_DENSE_MATRIX_ROWS))
      } text (s"s/blockSize is an int property (default and max allowed = $MAX_DENSE_MATRIX_ROWS")
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
    rdd.map(item => generate3(item, config.blocks, config.blockSize))
  }

  def generate3(id: Int, blocks: Int, blockSize: Int): BigMatrixXYZ = {
    Array.fill(blocks)(generate(id, blockSize))
  }

  def generate(id: Int, blockSize: Int): DenseMatrix[Double] = {
    val seed = System.nanoTime() / (id + 1)
    //np.random.seed(seed)
    //print(s"($id) generating $blockSize vectors...")
    val a = -1000
    val b = 1000
    val r = new scala.util.Random(seed)
    //val arr = Array.fill(blockCount, 3)(a + (b - a) * r.nextDouble)
    DenseMatrix.fill(blockSize, 3)(a + (b - a) * r.nextDouble)
  }

  def doShift(a: RDD[BigMatrixXYZ]): RDD[BigMatrixXYZ] = {
    val shift = DenseVector(25.25, -12.125, 6.333)
    a.map(x => add_xyz_vector(x, shift))
  }

  def add_xyz_vector(array: BigMatrixXYZ, shift: DenseVector[Double]): BigMatrixXYZ = {
    require {
      array.length > 0 && array(0).cols == shift.length
    }
    for (outer <- 0 until array.length) {
      for (inner <- 0 until array(outer).rows)
        array(outer)(inner, ::) :+= Transpose(shift)
    }
    array
  }

  case class Report(mapTime: Double, shiftTime: Double) {
    def toXML(): xml.Node = {
      <report>
        <time id="mapTime" t={ mapTime.toString } unit="ns"/>
        <time id="shiftTime" t={ shiftTime.toString } unit="ns"/>
      </report>
    }

    def toJSON(): org.json4s.JsonAST.JObject = {
      val timeData = ("mapTime" -> mapTime.toString) ~ ("shiftTime" -> shiftTime.toString)
      ("report" -> timeData)
    }
  }

  // command-line parameters

  case class Config(
      src: Option[String] = None,
      dst: Option[String] = None,
      cores: Int = 12,
      generate: Boolean = false,
      blocks: Int = 1,
      blockSize: Int = MAX_DENSE_MATRIX_ROWS, // 1GB contiguous matrices are used and multiplied using blocks.
      nparts: Int = 1,
      size: Int = 1,
      nodes: Int = 1,
      jsonFilename: Option[String] = None,
      xmlFilename: Option[String] = None
  ) {

    def toXML(): xml.Elem = {
      <config>
        <property key="src" value={ src.getOrElse("") }/>
        <property key="dst" value={ src.getOrElse("") }/>
        <property key="cores" value={ cores.toString }/>
        <property key="generate" value={ generate.toString }/>
        <property key="blocks" value={ blocks.toString }/>
        <property key="blockSize" value={ blockSize.toString }/>
        <property key="nparts" value={ nparts.toString }/>
        <property key="size" value={ size.toString }/>
        <property key="nodes" value={ nodes.toString }/>
        <property key="json" value={ jsonFilename.getOrElse("") }/>
        <property key="xml" value={ xmlFilename.getOrElse("") }/>
      </config>
    }

    def toJSON(): org.json4s.JsonAST.JObject = {
      val properties = ("src" -> src.getOrElse("")) ~ ("dst" -> dst.getOrElse("")) ~ ("cores" -> cores.toString) ~
        ("generate" -> generate.toString) ~ ("blocks" -> blocks.toString) ~ ("blockSize" -> blockSize.toString) ~
        ("nparts" -> nparts.toString) ~ ("size" -> size.toString) ~ ("nodes" -> nodes.toString) ~
        ("jsonFilename" -> jsonFilename.getOrElse("")) ~ ("xmlFilename" -> xmlFilename.getOrElse(""))
      ("config" -> properties)
    }

  }

}
