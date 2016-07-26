/*
 * SimpleMap: benchmark that shows how to work with binary data files and perform an inplace vector
 * shift. This uses Breeze DenseVector and stays in vector as long as possible until a DenseMatrix is
 * actually needed.
 */
package edu.luc.cs

import org.apache.spark.SparkContext
import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD
import scala.util.{ Try, Success, Failure }
import java.io._
import breeze.linalg._

// Not using these yet. Keeping for future reporting work.
import org.json4s._
import org.json4s.jackson._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._

object SimpleMap {

  def main(args: Array[String]) {
    val config = parseCommandLine(args).getOrElse(Config())
    val sc = new SparkContext()

    val aOpt =
      if (config.generate) {
        Some(rddFromGenerate(sc, config))
      } else if (config.src != None) {
        createResultsDir(config.dst.getOrElse("."), "/results")
        Some(rddFromBinaryFile(sc, config))
      } else {
        println("Either --src or --generate must be specified")
        sc.stop
        None
      }

    val a = aOpt.get
    val b = doShift(a)

    // TODO: Still working on adding performance/timing/results info.

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
      opt[String]('g', "generate") action { (_, c) =>
        c.copy(generate = true)
      } text ("g/generate is a Boolean property")
      opt[String]('d', "dst") action { (x, c) =>
        c.copy(dst = Some(x))
      } text ("d/dst is a String property")
      opt[Int]('b', "blocks") action { (x, c) =>
        c.copy(blocks = x)
      } text ("b/blocks is a int property")
      opt[Int]('s', "block_size") action { (x, c) =>
        c.copy(blockSize = x)
      } text ("s/blockSize is an int property")
      opt[Int]('n', "nodes") action { (x, c) =>
        c.copy(nodes = x)
      } text ("n/nodes is an int property")
      opt[Int]('p', "nparts") action { (x, c) =>
        c.copy(nparts = x)
      } text ("p/nparts is an int property")
      opt[Int]('z', "size") action { (x, c) =>
        c.copy(size = x)
      } text ("z/size is an int property")
      opt[Int]('c', "cores") action { (x, c) =>
        c.copy(cores = x)
      } text ("c/cores is an int property (default to 12 for dual-hexcore on Cooley)")

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

  def rddFromGenerate(sc: SparkContext, config: Config): RDD[DenseVector[Double]] = {
    val rdd = sc.parallelize(0 to config.blocks, config.nodes * config.cores * config.nparts)
    val gen_block_count = (config.blockSize * 1E6 / 24).toInt // 24 bytes per vector
    rdd.map(item => generate(item, gen_block_count))
  }

  def generate(id: Int, blockCount: Int): DenseVector[Double] = {
    val seed = System.nanoTime() / (id + 1)
    //np.random.seed(seed)
    print(s"($id) generating $blockCount vectors...")
    val a = -1000
    val b = 1000
    val r = new scala.util.Random(seed)
    //val arr = Array.fill(blockCount, 3)(a + (b - a) * r.nextDouble)
    DenseVector.fill(blockCount * 3)(a + (b - a) * r.nextDouble)
  }

  def rddFromBinaryFile(sc: SparkContext, config: Config): RDD[DenseVector[Double]] = {
    val rdd = sc.binaryFiles(config.src.get)
    rdd.map(binFileInfo => parseVectors(binFileInfo))
  }

  def nextDoubleFromStream(dis: DataInputStream): (Boolean, Double) = {
    Try {
      dis.readDouble
    } match {
      case Success(f) => (true, f)
      case Failure(f) => (false, Double.NaN)
    }
  }

  def parseVectors(binFileInfo: (String, PortableDataStream)): DenseVector[Double] = {
    val (path, bin) = binFileInfo
    val dis = bin.open()
    val iter = Iterator.continually(nextDoubleFromStream(dis))
    // Looks like DenseVector can initialize from an (infinite/indefinite) iterator!
    val scalaArray = iter.takeWhile(_._1).map(_._2).toArray
    DenseVector(scalaArray)
  }

  def doShift(a: RDD[DenseVector[Double]]): RDD[DenseVector[Double]] = {
    val shift = DenseVector(25.25, -12.125, 6.333)
    a.map(x => add_xyz_vector(x, shift))
  }

  def add_xyz_vector(vector: DenseVector[Double], shift: DenseVector[Double]): DenseVector[Double] = {
    val n = vector.length / shift.length
    require {
      vector.length % shift.length == 0
    }
    for (i <- 0 to n) {
      val low = i * shift.length
      val high = low + shift.length - 1
      // one of the few places where we go mutable to avoid copying a large array
      vector(low to high) :+= shift
    }
    vector
  }

  // command-line parameters

  case class Config(src: Option[String] = None, dst: Option[String] = None, cores: Int = 12,
    generate: Boolean = false, blocks: Int = 0, blockSize: Int = 0,
    nparts: Int = 1, size: Int = 1, nodes: Int = 1)

}
