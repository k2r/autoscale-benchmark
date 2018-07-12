#!/bin/sh

echo 'Cleaning class directory...'
mvn clean

echo 'Compiling sources and building jar with dependencies...'
mvn assembly:assembly

echo 'Copying jar in test repository'
cp target/stormBench-0.0.1-SNAPSHOT-jar-with-dependencies.jar $BENCHMARK/benchTopologies.jar

echo 'Test topologies updated!'