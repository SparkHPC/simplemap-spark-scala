#!/bin/bash

# Note: This script is not for general use.
# Might be generalized at some point but this is specific to my work enviornment.
# Note: Need to increase default time based on experiment size.

# usage: each argument should be a number of nodes experiment that you wish to run. For example,
# if you want to run sizes 1 4 and 8, do "run-experiments.sh 1 4 8" from the top-level folder
# simplemap-spark-scala

allocation=IME_BlockCoPolymers
net=pubnet

# If user wants to set the initial dependencies, set the jobid before running.
if [ "" != "$jobid" ]; then
     dependencies="--dependencies $jobid"
else
     dependencies=""
fi

if [ "" == "$jobtime" ]; then
     jobtime=01:00:00
fi

for nodes in $@; do
  for script in ./qscripts.d/$nodes/*1parts*.sh ; do
     echo qsub -n $nodes -t $jobtime -A $allocation -q $net $dependencies $script
     jobid=$(qsub -n $nodes -t $jobtime -A $allocation -q $net $dependencies $script)
     dependencies="--dependencies $jobid"
  done
done
