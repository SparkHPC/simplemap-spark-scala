/*
 * SimpleMap: Large array tests with RDDs using Breeze DenseMatrix[Double] 
 * for the block representation of array of (x, y, z) data. We use Array
 * of DenseMatrix[Double] to scale up the data size, since Java arrays (and
 * Scala by association) are presently limited to 32-bit size.
 *
 * Use --blocks parameter to multiply --block_size to get bigger arrays!
 */
package dataflows.spark

import blockperf._
import java.io._
import breeze.linalg._
import SparkBenchmarkHPC._

object KernelBenchmark {

  def main(args: Array[String]) {
    val config = parseCommandLine(args).getOrElse(Config())

    val (generateTime, _, array) = performance {
      generate(0, config.blockSize, config.multiplier)
    }

    val (shiftTime, _, shifted) = performance {
      val shift = DenseVector(25.25, -12.125, 6.333)
      addVectorDisplacement(array, shift)
    }

    val (avgTime, _, c) = performance {
      averageOfVectors(shifted)
    }

    printf("rows: %d, cols: %d\n", array.length * array(0).rows, array(0).cols)
    printf("generation: %f\n", generateTime.t / 1e9)
    printf("shift: %f\n", shiftTime.t / 1e9)
    printf("average: %f\n", avgTime.t / 1e9)
  }
}
