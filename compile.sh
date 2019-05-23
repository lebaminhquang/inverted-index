#!/bin/bash
path=$PWD
hadoopvar=/home/minhquang/hadoop/bin/hadoop
#$1 = times 

#hdfs dfs -mkdir /data_naive
#hdfs dfs -mkdir /data_naive/input
#hdfs dfs -mkdir /data_naive/input2
#hdfs dfs -mkdir /output_naive

# Compile java file
cd $path/src
$hadoopvar com.sun.tools.javac.Main MR1.java
jar cf MR1.jar MR1*.class

$hadoopvar com.sun.tools.javac.Main MR2.java
jar cf MR2.jar MR2*.class

