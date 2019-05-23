#!/bin/bash
path=$PWD
hadoopvar=/home/minhquang/hadoop/bin/hadoop
#$1 = times 

# Remove exist file in hdfs
hdfs dfs -rm /data_naive/input/*
hdfs dfs -rm /data_naive/input2/*

# Put from local to hdfs
hdfs dfs -put $path/input/*  /data_naive/input

# Run
cd $path/src
$hadoopvar jar MR1.jar MR1 /data_naive/input /output_naive/output0$1

# Copy from HSDF to local
rm $path/res/*
hdfs dfs -get /output_naive/output0$1/part* $path/res

# Run MR-2
$hadoopvar jar MR2.jar MR2 /output_naive/output0$1 /output_naive/output-MR2-0$1 /data/src/query.txt

# Copy from HSDF to local
hdfs dfs -get /output_naive/output-MR2-0$1/part* $path/res/resMR2.txt

