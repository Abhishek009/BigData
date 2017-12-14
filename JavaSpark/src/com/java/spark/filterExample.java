package com.java.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/* 
 * Return a new dataset formed by selecting those elements of the source on which func returns true. 
 */
public class filterExample {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Filter Example");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> data = sc.textFile(args[0]);
		JavaRDD<String> filter = data.filter(f->f.contains("me"));
		filter.collect().forEach(f->System.out.println(f));
		
	}

}
