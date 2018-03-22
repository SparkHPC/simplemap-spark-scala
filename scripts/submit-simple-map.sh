#/bin/bash

# This script isn't intended to be for general users.
# There are a bunch of parameters, which have defaults you can override with command line options.



# qsub info

# allocation time in minutes (int)
# -t
MINUTES=120

# number of nodes (int)
# -n
NODES=128

# allocation (string)
# -a
ALLOCATION=datascience

# queue (string)
# -q
QUEUE=default

# app info

# number of blocks to create (int)
# -b
BLOCKS=2048

# block size in MB (int)
# -s
BLOCK_SIZE=64

# partition multiplier (best to leave at 1)
# -p
NPARTS=1

# cores (best to leave at 128)
# -c
CORES=128

# label (string)
# -l
LABEL=theta


# working dir (path to Spark job submission scripts)
# -W
WORKING_DIR=~/Work/Cooley_Spark

# None of this is configurable, since this script is app-specific

# app dir (path to simplemap-spark-scala)
# -A
APP_DIR=~/Work/simplemap-spark-scala
ASSEMBLY=$(find "$APP_DIR" -name '*.jar'|head -n1)


while getopts ":a:n:t:q:b:s:p:c:l:" opt; do
  case $opt in
    a)
      ALLOCATION=$OPTARG
      ;;
    n)
      NODES=$OPTARG
      ;;
    t)
      MINUTES=$OPTARG
      ;;
    q)
      QUEUE=$OPTARG
      ;;

    b)
      BLOCKS=$OPTARG
      ;;
    s)
      BLOCK_SIZE=$OPTARG
      ;;

    p)
      PARTS=$OPTARG
      ;;
    
    c)
      CORES=$OPTARG
      ;;
    l)
      LABEL=$OPTARG
      ;;
    W)
      WORKING_DIR=$OPTARG
      ;;

    A)
      APP_DIR=$OPTARG
      ;;
    \?)
      echo "Invalid option: -$OPTARG"
      exit 1
      ;;
    :)
      echo "Option -$OPTARG requires an argument."
      exit 1
      ;;
  esac

done

echo "Done with arguments"

if [ ! -d "$WORKING_DIR" ]; then
  echo "Cannot find Cooley/Theta Spark scripts"
  exit 1
fi

REPORT_DIR=$APP_DIR/experiments/$LABEL/simplemap
mkdir -p $REPORT_DIR
REPORT_NAME=$REPORT_DIR/simplemap-$NODES-$BLOCKS-$BLOCK_SIZE-$NPARTS-$CORES.json
echo "Report will be written to $REPORT_NAME"
pushd $WORKING_DIR

./submit-spark.sh -n $NODES -A $ALLOCATION -t $MINUTES -q $QUEUE \
  $ASSEMBLY \
  --generate \
  --blocks $BLOCKS \
  --block_size $BLOCK_SIZE \
  --nodes $NODES \
  --nparts $NPARTS --cores $CORES \
  --json $REPORT_NAME

popd
