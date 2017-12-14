package com.java.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class mapExample {

	public static void main(String args[]) {

		String master, inputpath;
		if (args.length < 2) {
			System.out.println("Insufficient parameter.");
			System.exit(1);
		} else {
			master = args[0];
			inputpath = args[1];
			SparkConf conf = new SparkConf().setAppName("mapExample").setMaster(master);
			JavaSparkContext sc = new JavaSparkContext(conf);
			JavaRDD<String> rdd = sc.textFile(inputpath);
			JavaRDD<Integer> mapdata = rdd.map(f -> Integer.parseInt(f) * Integer.parseInt(f));
			mapdata.collect().forEach(x -> System.out.println(x));
		}
	}
}
