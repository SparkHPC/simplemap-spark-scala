/*
 * This generates bash scripts for testing performance across various combinations 
 * of parameters (e.g. nodes, partitions, blocks and block size).
 *
 * The resulting scripts can be submitted to Cobalt via qsub. 
 *
 * The scripts start Apache Spark and run the assembly all-in-one.
 */

package dataflows.spark

import scala.util.{ Try, Success, Failure }
import java.io._

object GenerateBashScripts {

  val cores = 12
  val scriptBaseDir = new File("qscripts.d")

  def main(args: Array[String]) {
    val scripts = large() ++ strong()
    scripts map { script => script.save } filter { _.isDefined } map { _.get } foreach println
  }

  // generate shell scripts to run Spark experiments
  def large(): Iterator[Script] = {
    for {
      // blocks are allocated in 1MB chunks in the benchmark (1GB to 32GB here)
      blockSize <- List(1, 4, 8, 16).map(gb => gb * 1024).iterator

      // nodes on Cooley (we stop at 100 since it is almost impossible to get 120+)
      nodes <- List(1, 4, 8, 16, 32, 64, 96)

      // partition multiplier (leave at 1 for now unless you want to spill more data)
      nparts <- List(1)

      // blocks (this gives us one block per partition when nparts=1)
      blocks <- List(nparts * nodes * cores)
    } yield {
      Script("large", nodes, nparts, blocks, blockSize)
    }
  }

  def strong(): Iterator[Script] = {
    for {
      // blocks are allocated in 1MB chunks in the benchmark (1GB to 32GB here)
      blockSize <- List(64).iterator

      // nodes on Cooley (we stop at 100 since it is almost impossible to get 120+)
      nodes <- List(1, 2, 4, 8, 16)

      // partition multiplier (leave at 1 for now unless you want to spill more data)
      nparts <- List(1, 4)

      // blocks (this gives us one block per partition when nparts=1)
      blocks <- 4 to 15 map { math.pow(2, _).toInt }
    } yield {
      Script("strong", nodes, nparts, blocks, blockSize)
    }
  }

  case class Script(groupName: String, nodes: Int, nparts: Int, blocks: Int, blockSize: Int) {
    val groupDir = new File(scriptBaseDir, groupName)
    val dir = new File(groupDir, s"$nodes")
    val filename = s"do-simplemap-${nodes}nodes-${nparts}parts-${blocks}blocks-${blockSize}MB.sh"

    val scriptHeader = s"""#!/bin/bash
      |# Generated by Cobalt GenerateBasScripts
      |#
      |# Parameters (for debugging purposes):
      |#
      |# nodes = $nodes
      |# nparts = $nparts
      |# blocks = $blocks
      |# blockSize = $blockSize
      |# cores = $cores
      |#""".stripMargin

    val startSpark = s"""
      |#
      |# Start Apache Spark Cluster
      |#
      |
      |JOB_LOG=$$HOME/logs/$$COBALT_JOBID.txt
      |JOB_JSON=$$HOME/logs/$$COBALT_JOBID.json
      |JOB_XML=$$HOME/logs/$$COBALT_JOBID.xml
      |
      |pushd $$HOME/code/spark
      |cat $$COBALT_NODEFILE > conf/slaves
      |cat $$COBALT_NODEFILE >> $$JOB_LOG
      |./sbin/start-all.sh
      |NODES=`wc -l conf/slaves | cut -d" " -f1`
      |popd
      |
      |MASTER=`hostname`
      |
      |echo "# Spark is now running with $$NODES workers:" >> $$JOB_LOG
      |echo "#"
      |echo "export SPARK_STATUS_URL=http://$$MASTER.cooley.pub.alcf.anl.gov:8000" >> $$JOB_LOG
      |echo "export SPARK_MASTER_URI=spark://$$MASTER:7077" >> $$JOB_LOG
      |
      |SPARK_MASTER_URI=spark://$$MASTER:7077
      |SPARK_HOME=$$HOME/code/spark
      |
      |#
      |# Done Initializing Apache Spark
      |#""".stripMargin

    val submitSparkJob = s"""
      |#
      |# Submit Application on Spark
      |#
      |
      |for ASSEMBLY in $$(find . -type f -name simplemap-spark-scala-assembly*.jar)
      |do
      |   echo "Running: "$$SPARK_HOME/bin/spark-submit \\
      |      --master $$SPARK_MASTER_URI $$ASSEMBLY \\
      |      --generate --blocks $blocks --block_size $blockSize --nodes $nodes \\
      |      --nparts $nparts --cores $cores \\
      |      --json $$JOB_JSON >> $$JOB_LOG
      |
      |   $$SPARK_HOME/bin/spark-submit \\
      |      --master $$SPARK_MASTER_URI $$ASSEMBLY \\
      |      --generate --blocks $blocks --block_size $blockSize --nodes $nodes \\
      |      --nparts $nparts --cores $cores \\
      |      --json $$JOB_JSON >> $$JOB_LOG
      |done
      |
      |#
      |# Done Submitting Application on Spark
      |#
      |""".stripMargin

    val body = scriptHeader + startSpark + submitSparkJob

    def save(): Option[String] = {
      Try {
        dir.mkdirs
      }
      if (dir.exists) {
        val file = new File(dir, filename)
        val bw = new BufferedWriter(new FileWriter(file))
        bw.write(body)
        bw.close
        Some(file.getAbsolutePath)
      } else {
        None
      }
    }
  }

}
