package com.java.spark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/*
 * Write the elements of the dataset in a simple format using Java serialization, 
 * which can then be loaded using SparkContext.objectFile(). 
 */
public class saveAsObjectFileExample {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setMaster("local").setAppName("Save as Object");
		JavaSparkContext sc = new JavaSparkContext(conf);
		String firstPath = args[0];
		String fileRead = args[1];
		String secondPath = args[2];
		//Creating RDD 
		JavaRDD<Integer> data = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),2);
		//Save file as object.
		data.saveAsObjectFile(firstPath);
		//Read data from a file
		JavaRDD<String> datafile = sc.textFile(fileRead,2);
		//Save file as object
		datafile.saveAsObjectFile(secondPath);
		System.out.println("File saved");

	}

}
