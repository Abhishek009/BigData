package com.java.spark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class worldCountExample {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("worldCount");
		JavaSparkContext sc = new JavaSparkContext(conf);
		// Creating Java RDD
		JavaRDD<String> data = sc.textFile(args[0]);
		// Spliting every world of the file.
		JavaRDD<String> splitData = data.flatMap(f->Arrays.asList(f.split(" ")).iterator());
		// For every world setting a value as 1.
		JavaPairRDD<String, Integer> pairData = splitData.mapToPair(f->new Tuple2(f, 1));
		// Reducing the data on basis of key and sum up its values.
		JavaPairRDD<String, Integer> reduceData = pairData.reduceByKey((key,value)->key+value);
		//Save the output to a file
		reduceData.saveAsTextFile(args[1]);

	}

}
