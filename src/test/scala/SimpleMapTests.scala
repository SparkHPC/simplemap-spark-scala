import org.scalatest._

import breeze.linalg._
import dataflows.spark.SparkBenchmarkHPC._

/*

Initial efforts to test RDDs. Doesn't seem to work with all ScalaTest frameworks.

import com.holdenkarau.spark.testing._
import org.scalatest.Assertions._

class SampleRDD extends FunSuite with SharedSparkContext {
  test("really simple transformation") {
    val input = List("hi", "hi cloudera", "bye")
    val expected = List(List("hi"), List("hi", "cloudera"), List("bye"))

  }
}
*/

class SparkBechmarkHPCTests extends FlatSpec with Matchers {

  "Breeze Kernel" should "initialize from Scala Array" in {
    val a = Array(1.0, 2.0, 3.0)
    val dv = DenseVector(a)
    dv should be(DenseVector(1.0, 2.0, 3.0))
  }

  "SparkBenchmarkHPC" should "generate wrapped 32-bit arrays" in {
    val args = "--generate --blocks 1 --block_size 10 --multiplier 16384".split(" ")
    val config = parseCommandLine(args).get
    val data = generate(1, config.blockSize, config.multiplier)
    data.length should be(config.blockSize)
    for (i <- 0 until data.length) {
      data(i).rows should be(config.multiplier)
      data(i).cols should be(3)
    }
  }

  it should "parse command line" in {
    val args = "--generate --blocks 1 --block_size 10 --multiplier 16384 --nodes 4 --nparts 10 --cores 12 --json output.json --xml output.xml".split(" ")
    val config = parseCommandLine(args).get
    config.generate should be(true)
    config.blocks should be(1)
    config.blockSize should be(10)
    config.nodes should be(4)
    config.cores should be(12)
    config.multiplier should be(16384)
    config.jsonFilename.get should be("output.json")
    config.xmlFilename.get should be("output.xml")
    config.src should be(None)
    config.dst should be(None)
  }

  // TODO: Might rework this using ScalaCheck (if worth it)

  "Scala" should "generate distribution of doubles across range" in {
    val gen = RandomDoubles(0, -3, 3)
    val data = Array.fill(10000)(gen.next)
    for (bucket <- -2 to 2) {
      data map { item => item.toInt } count (value => value == bucket) should be > 0
    }
  }
}
