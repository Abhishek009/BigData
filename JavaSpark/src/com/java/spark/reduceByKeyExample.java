package com.java.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
/*	
 * When called on a dataset of (K, V) pairs, returns a dataset of (K, V) pairs where the values for each key are 
 * aggregated using the given reduce function func, which must be of type (V,V) => V. 
 */

/*
 Note on reduceByKey Transformations function
 1. It can only be used with RDDs which contains key and value pairs kind of elements
 2. It accepts a Commutative and Associative function as an argument
 3. The parameter function should have two arguments of the same data type
 4. The return type of the function also must be same as argument types
 */

public class reduceByKeyExample {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("ReduceByKey");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> data = sc.textFile(args[0]);
		// mapToPair function will map JavaRDD to JavaPairRDD
		JavaPairRDD<String, Integer> rddX =data.mapToPair(e -> new Tuple2<String, Integer>(e, 1));
		// New JavaPairRDD 
		JavaPairRDD<String, Integer> rddY = rddX.reduceByKey((key,value)->key+1);
		//Print tuples
		for(Tuple2<String, Integer> element : rddY.collect()){
			System.out.println("("+element._1+", "+element._2+")");
		}
		
	}

}
