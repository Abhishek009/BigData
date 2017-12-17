package com.java.spark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/*
 * Write the elements of the dataset as a text file (or set of text files) in a given directory in 
 * the local filesystem,HDFS or any other Hadoop-supported file system. 
 * Spark will call toString on each element to convert it to a line of text in the file. 
 */
public class saveAsTextFileExample {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setMaster("local").setAppName("ReduceByKey");
		JavaSparkContext sc = new JavaSparkContext(conf);
		String firstPath = args[0];
		String fileRead = args[1];
		String secondPath = args[2];
		// Creating RDD
		JavaRDD<Integer> data = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 2);
		// Save as text file
		data.saveAsTextFile(firstPath);
		// Read file data
		JavaRDD<String> datafile = sc.textFile(fileRead, 2);
		// Save data as object file
		datafile.saveAsTextFile(secondPath);
		System.out.println("File saved");

	}

}
