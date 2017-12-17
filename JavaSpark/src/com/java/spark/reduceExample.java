package com.java.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class reduceExample {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("ReduceByKey");
		JavaSparkContext sc = new JavaSparkContext(conf);
		//Creating RDD
		JavaRDD<String> data = sc.textFile(args[0]);
		//Converting RDD to integer type.
		JavaRDD<Integer> intdata = data.map(f -> Integer.parseInt(f));
		//Applying reduce action 
		Integer rdata = intdata.reduce((accum, n)->accum+n);
		System.out.println(rdata);
		

	}

}
