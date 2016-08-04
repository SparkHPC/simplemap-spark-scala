# For this to work in your environment, you'll have to specify these variables
# on the command line to override.

SHELL=/bin/bash

all:
	sbt assembly

jobs:
	sbt "run-main edu.luc.cs.GenerateBashScripts"
	chmod -R u+x scripts/

sbt_clean:
	sbt clean

fs_clean:
	rm -f *.error *.cobaltlog *.output
	rm -f ~/logs/*
	find . -type d -name target -print | xargs rm -rf
	rm -rf scripts/

clean:
	make sbt_clean
	make fs_clean
