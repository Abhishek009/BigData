package com.java.spark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

/*
 * File streaming world count 
 */

public class fileStreamExample {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setMaster("local").setAppName("File Stream");
		JavaSparkContext sc = new JavaSparkContext(conf);
		String dir = args[0];
		// Creating java streaming contect with a time window of 10 sec
		JavaStreamingContext jsc = new JavaStreamingContext(sc,new Duration(10000));
		// Reading the dirtectory as a streaming source
		JavaDStream<String> data = jsc.textFileStream(dir);
		// Spliting every world of the file.
		JavaDStream<String> fdata = data.flatMap(f->Arrays.asList(f.split(" ")).iterator());
		// For every world setting a value as 1.
		JavaPairDStream<String, Integer> mapData = fdata.mapToPair(f->new Tuple2(f, 1));
		// Reducing the data on basis of key and sum up its values.
		JavaPairDStream<String, Integer> reduceData = mapData.reduceByKey((k,v)->k+v);
		reduceData.print();
		
		//Starting the streaming
		jsc.start();
		try {
			jsc.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
