Overview
-----------

SimpleMap is one of the benchmark programs developed by Cameron Christensen at https://github.com/cchriste/dataflow.

This is a 100% Scala version by George K. Thiruvathukal (with a few additions).

We use the Breeze Linear Algebra DenseVector (at the moment) to give a comparable experience to NumPy.

The benchmark performs a simple vector shift on either a generated data set or a supplied set of files.

In addition, this version shows how to do performance reporting (using JSON and XML) for a posteriori analysis.

We also generate self-contained scripts for submitting Apache Spark itself and the benchmark .jar file as a Spark job.
This templates in GenerateBashScripts are inspired by Cameron's work on setting up scripts to start Spark using Cobalt.


