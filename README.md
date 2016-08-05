Overview
-----------

SimpleMap is one of the benchmark programs developed by Cameron Christensen at https://github.com/cchriste/dataflow.

This is a 100% Scala version by George K. Thiruvathukal (with a few additions).

We use the Breeze Linear Algebra DenseVector (at the moment) to give a comparable experience to NumPy.

The benchmark performs a simple vector shift on either a generated data set or a supplied set of files.

In addition, this version shows how to do performance reporting (using JSON and XML) for a posteriori analysis.

We also generate self-contained scripts for submitting Apache Spark itself and the benchmark .jar file as a Spark job.
This templates in GenerateBashScripts are inspired by Cameron's work on setting up scripts to start Spark using Cobalt.

Usage
------

The benchmark is fully parameterized via the command line:
```
$SPARK_HOME/bin/spark-submit --master $SPARK_MASTER_URI target/scala-2.10/simplemap-spark-scala-1.0.jar  --help
```

This gives you help.

```
simplemap-spark-scala 0.1.x
Usage: scopt [options]

  -s <value> | --src <value>
        s/src is a String property
  -g | --generate
        g/generate is a Boolean property
  -d <value> | --dst <value>
        d/dst is a String property
  -b <value> | --blocks <value>
        b/blocks is a int property
  -s <value> | --block_size <value>
        s/blockSize is an int property
  -n <value> | --nodes <value>
        n/nodes is an int property
  -p <value> | --nparts <value>
        p/nparts is an int property
  -c <value> | --cores <value>
        c/cores is an int property (default to 12 for dual-hexcore on Cooley)
  -j <value> | --json <value>
        json <filename>is where to write JSON reports
  -x <value> | --xml <value>
        xml <filename> is where to write XML reports
  --help
        prints this usage text
```

There are basically two modes of operation:

- operate from generated data sets

- operate from existing binary files


The frictionless path is to start by generating datasets using `--generate`. In this case, you'll want to 
indicate `--blocks` and `--block_size`. Most of the options are pretty self-explanatory and help Apache Spark
to take advantage of various resources (e.g. nodes and cores). Cores shoud be set based on your CPU's total
number of cores. Nodes should be set to the number of nodes in your cluster, if you have one set up.

The `--json` and `--xml`` options are for logging all configuration parameters and performance results to
the corresponding type of file.


More to come.

