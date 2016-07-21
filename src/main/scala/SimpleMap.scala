package edu.luc.cs

import java.nio.file.Paths

import org.apache.spark.SparkContext
import java.io._

import org.apache.spark.rdd.RDD

import scala.util.Try

object SimpleMap {

  case class Config(src: Option[String] = None, dst: Option[String] = None,
                    blocks: Int = 0, blockSize: Int = 0, nparts: Int = 1,
                    size: Int = 1, nodes: Int = 1)

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
      help("help") text ("prints this usage text")

    }
    // parser.parse returns Option[C]
    parser.parse(args, Config())
  }

  def createResultsDir(dst: String, resultsDirName: String): Boolean = {
    val outdir = new File(dst, resultsDirName)
    Try { outdir.mkdirs() } getOrElse (false)
  }

  def add_vec3_in_place(arr: Array[Array[Double]], vec: Array[Double]): Array[Array[Double]] = {
    for (i <- 0 to arr.length)
      for (j <- 0 to vec.length)
        arr(i)(j) += vec(j)
    vec
  }

  def add_vec3_zipped(arr: Array[Array[Double]], vec: Array[Double]): Array[Array[Double]] = {
    for (i <- 0 to arr.length)
      arr(i) = (arr(i), vec).zipped.map(_ + _)
    vec
  }

  def generate(x: Int, blockCount: Int): Array[Array[Double]] = {
    val seed = System.nanoTime() / (x + 1)
    //np.random.seed(seed)
    print(s"($x) generating $blockCount vectors...")
    val a = -1000
    val b = 1000
    val r = new scala.util.Random(seed)
    val arr = Array.fill(blockCount, 3)(r.nextDouble)
    arr
  }

  def rddFromGenerate(sc: SparkContext, config: Config): RDD[(Array[Array[Double]])] = {
    val rdd = sc.parallelize(0 to config.blocks, config.nodes * 12 * config.nparts)
    val gen_block_count = (config.blockSize * 1E6 / 24).toInt // 24 bytes per vector
    rdd.map(item => generate(item, gen_block_count))
  }

  def rddFromFile(sc: SparkContext, config: Config): RDD[(Array[Array[Double]])] = {
    // TODO: Port code to read from file.
    rddFromGenerate(sc, config)
  }

  def main(args: Array[String]) {
    val config = parseCommandLine(args).getOrElse(Config())
    val sc = new SparkContext()

    createResultsDir(config.dst.getOrElse("."), "/results")

    val a =
      if (config.blocks > 0 && config.blockSize > 0)
        rddFromGenerate(sc, config)
      else
        rddFromFile(sc, config)

    val shift = Array(25.25, -12.125, 6.333)
    val b = a.map(x => add_vec3_in_place(x, shift))
  }
}
