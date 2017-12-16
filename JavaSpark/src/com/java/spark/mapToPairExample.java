package com.java.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class mapToPairExample {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Map To Pair");
		JavaSparkContext sc = new JavaSparkContext(conf);
		//Creating RDD. Since its a text file so it will create one line as an one RDD.
		JavaRDD<String> data = sc.textFile(args[0]);
		data.collect().forEach(f->System.out.println(f));
		//Converting to paired RDD
		JavaPairRDD<String, Integer> pairData = data.mapToPair(f -> new Tuple2<String, Integer>(f, 1));
		pairData.collect().forEach(f->System.out.println("Key:"+f._1+" Value:"+f._2));

	}

}
