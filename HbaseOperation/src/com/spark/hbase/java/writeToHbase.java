package com.spark.hbase.java;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;

public class writeToHbase {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("CSV to DataFrame");
		JavaSparkContext sparkContext = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sparkContext);
		String path = args[0];
		
/*		LTE|E|201709050000|201709050000|518400|CLL00066|CLL00066_2S_1_DB|201709050000|201709060000|7|CXP102051/26_R27BF|0|0|0|0|
		LTE|E|201709050000|201709050000|777600|CLL04004|CLL04004_7R_1_DB|201709050000|201709060000|7|CXP9024418_5 R18CB|0|0|0|0|
*/		
		
		
		StructType schema = new StructType()
		.add("name","string")
		.add("Age","int")
		.add("endstamp","string");
		
		System.out.println("Path ---> "+path);
		
		/*     Configuration dhrconf = HBaseConfiguration.create();
		    dhrconf.addResource(new Path("/etc/hbase/2.6.0.3-8/0/hbase-site.xml"));
		    dhrconf.addResource(new Path("/usr/hdp/2.6.0.3-8/hadoop/etc/hadoop/core-site.xml"));
		    dhrconf.set("hbase.client.retries.number","10");*/
		    
		    //Setting counter table to read conf
		  //  dhrconf.set(TableInputFormat.INPUT_TABLE, HTableName);
		
		DataFrame df = sqlContext.read().schema(schema).format("com.databricks.spark.csv")
				.option("delimiter", "|")
				.option("inferSchema", "true")
				.load(path);
		df.show();
		
		df.write()
		  .format("org.apache.phoenix.spark") 
		  .mode("overwrite") 
		  .option("table", "INPUT_TABLE") 
		  .option("zkUrl", "sandbox.hortonworks.com:2181") 
		  .save();

		//df.write().format("com.databricks.spark.csv").option("header", "true").save("file.csv");
		
	
	}

}
