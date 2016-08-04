#!/bin/bash

# Note: This script is not for general use.
# Might be generalized at some point but this is specific to my work enviornment.
# Note: Need to increase default time based on experiment size.

# usage: each argument should be a number of nodes experiment that you wish to run. For example,
# if you want to run sizes 1 4 and 8, do "run-experiments.sh 1 4 8" from the top-level folder
# simplemap-spark-scala

jobid=""
allocation=IME_BlockCoPolymers
net=pubnet
for nodes in $@; do
  for script in ./scripts/$nodes/*.sh; do
     if [ "$jobid" == "" ]; then
        dependencies=""
     else
        dependencies="--dependencies $jobid"
     fi
     echo qsub -n $nodes -t 00:15:00 -A $allocation -q $net $dependencies $script
     jobid=$(qsub -n $nodes -t 00:15:00 -A $allocation -q $net $dependencies $script)
  done
done
