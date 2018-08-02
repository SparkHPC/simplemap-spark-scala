Status
--------

[![Build Status](https://travis-ci.org/gkthiruvathukal/simplemap-spark-scala.svg?branch=master)](https://travis-ci.org/gkthiruvathukal/simplemap-spark-scala)

Overview
-----------

SimpleMap is one of the benchmark programs developed by Cameron Christensen at
https://github.com/cchriste/dataflow. This builds on work being done by Cameron Christensen, George K. Thiruvathukal, and Venkat Vishwanath to evaluate various data flows on supercomputers.

This version is a 100% Scala version by George K. Thiruvathukal (with a few additions).

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


Performance Logging
---------------------

Performance data is written in JSON and XML formats. This isn't strictly necessary for HPC work, but I prefer
working with more structured data in case I have to change the design later. For example, I may add other 
*phases* to the benchmark that have separate timings. With the design I have chosen, it's easy to add new
entries to the JSON and XML formats. 

Here is what a typical JSON performance object looks like:

```json
{
  "results" : {
    "experiment" : {
      "id" : "simplemap-spark-scala"
    },
    "config" : {
      "src" : "",
      "dst" : "",
      "cores" : "12",
      "generate" : "true",
      "blocks" : "2000",
      "blockSize" : "10",
      "nparts" : "192",
      "size" : "1",
      "nodes" : "16",
      "jsonFilename" : "/home/thiruvat/logs/883987.json",
      "xmlFilename" : "/home/thiruvat/logs/883987.xml"
    },
    "report" : {
      "mapTime" : "2.08551858E8",
      "shiftTime" : "2.27948975E8"
    }
  }
}
```

And here is the XML version of same:

```xml
<results>
  <experiment id="simplemap-spark-scala"/>
  <config>
    <property key="src" value=""/>
    <property key="dst" value=""/>
    <property key="cores" value="12"/>
    <property key="generate" value="true"/>
    <property key="blocks" value="2000"/>
    <property key="blockSize" value="10"/>
    <property key="nparts" value="192"/>
    <property key="size" value="1"/>
    <property key="nodes" value="16"/>
    <property key="json" value="/home/thiruvat/logs/883987.json"/>
    <property key="xml" value="/home/thiruvat/logs/883987.xml"/>
  </config>
  <report>
    <time id="mapTime" t="2.08551858E8" unit="ns"/>
    <time id="shiftTime" t="2.27948975E8" unit="ns"/>
  </report>
</results>
```

Performance Analysis
---------------------

We obviously don't work directly with JSON or XML when it comes to final analysis. The script `results2db.py` 
gathers all of the JSON data and stores it in an sqlite3 database. I realize that I could have gone straight
to a database from the SimpleMap (Scala) benchmark. Unfortunately, Scala doesn't offer a great choice for
working with embedded databases. Even so, I like the idea of logging my experimental results to a simple
native format that Scala *speaks well*. Being an internet language, Scala does a terrific job with JSON and XML
formats, and I can keep my code nicely platform independent.

So to create the sqlite3 database, do:

```
$ python results2db.py <directory>
```

In my work, I am logging all of the runs to a folder ~/logs. The `results2db.py` script gathers all files
ending in `.json` and stores them (flattened) in an sqlite3 database.

This creates a database named `simplemap-spark-scala.db`.

We can then do a query to see the results obtained for various combinations of parameters (or even a single
parameter).

For example, to see the results for all experiments run on 16 nodes:

```
$ sqlite3 -header -csv simplemap-spark-scala.db 'select * from results where nodes = 16;'
id,experiment_id,generate,nodes,cores,nparts,blocks,block_size,map_time,shift_time,job_id,src,dst
33,simplemap-spark-scala,1,16,12,192,1000,1000,273080253.0,225905341.0,883982,"",""
34,simplemap-spark-scala,1,16,12,192,1000,100,208410205.0,229238769.0,883983,"",""
35,simplemap-spark-scala,1,16,12,192,1000,10,216620976.0,238596571.0,883984,"",""
36,simplemap-spark-scala,1,16,12,192,2000,1000,213881191.0,236287899.0,883985,"",""
37,simplemap-spark-scala,1,16,12,192,2000,100,210983962.0,229351669.0,883986,"",""
38,simplemap-spark-scala,1,16,12,192,2000,10,208551858.0,227948975.0,883987,"",""
```

Or perhaps you want to see all situations where we ran with blocks = 1000 and block\_size = 1000:

```
sqlite3 -header -csv simplemap-spark-scala.db 'select * from results where blocks = 1000 and block_size = 1000;'
id,experiment_id,generate,nodes,cores,nparts,blocks,block_size,map_time,shift_time,job_id,src,dst
0,simplemap-spark-scala,1,1,12,12,1000,1000,204754300.0,226270579.0,883941,"",""
6,simplemap-spark-scala,1,1,12,12,1000,1000,205997693.0,232052270.0,883949,"",""
12,simplemap-spark-scala,1,4,12,48,1000,1000,207997276.0,233139221.0,883955,"",""
18,simplemap-spark-scala,1,8,12,96,1000,1000,211897826.0,230880054.0,883961,"",""
24,simplemap-spark-scala,1,8,12,96,1000,1000,209286166.0,232733133.0,883969,"",""
27,simplemap-spark-scala,1,8,12,96,1000,1000,209864819.0,230894542.0,883976,"",""
33,simplemap-spark-scala,1,16,12,192,1000,1000,273080253.0,225905341.0,883982,"",""
```

Kernel Benchmark Sans Spark
--------------------------------

Sometimes it is nice to test the _kernel_ computation being done in a Spark program independent of its RDD evaluation (owing to lazy behavior, etc.)
You can see how long various phases of the computation take (per RDD) by running the `dataflows.spark.KernelBenchmark` as follows:

Run kernel benchmark with block size of 10 * 1024 * 1024 (3d float vectors):

sbt "run-main dataflows.spark.KernelBenchmark -k 10"

Override the multiplier of 1024\*1024 by using -m or --multiplier:

sbt "run-main dataflows.spark.KernelBenchmark -k 10 -m 16384"

Sometimes it is more convenient to specify -m as the result of a calculation (e.g. some power of two KB or MB)

This is what I do using a Python one-liner:

MULTIPLIER=$(python -c "print(2 \*\* 20)") sbt "run-main dataflows.spark.KernelBenchmark -k 10 -m $MULTIPLIER"



