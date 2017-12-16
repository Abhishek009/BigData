package com.java.spark;

import java.awt.List;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class createRDDExample {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Create RDD");
		JavaSparkContext sc = new JavaSparkContext(conf);
		//Creating RDD from external source. 
		//Since its a text file so it will create one line as an one RDD.
		JavaRDD<String> data = sc.textFile(args[0]);
		System.out.println("Printing file data");
		data.collect().forEach(f->System.out.println(f));

		//Creating a list with numbers.
		ArrayList<String> list = new ArrayList<String>();
		for(int i=0;i<10;i++) {
			list.add(i+"");
		}
		//Creating RDD from collection.
		JavaRDD<String> listData = sc.parallelize(list);
		System.out.println("Printing list data");
		listData.collect().forEach(f->System.out.println(f));
	}

}
