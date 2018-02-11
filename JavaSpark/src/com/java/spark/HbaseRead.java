package com.java.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

/*
* Read data from hbase
*/
public class HbaseRead {

	public static void main(String[] args) {
		String tableName = null;
		
		if(args.length<1) {
			System.out.println("Insufficient parameter");
			System.exit(0);
		}else {
			tableName = args[0]; // Table name
			SparkConf sparkConf = new SparkConf().setAppName("HBaseRead");
			sparkConf.set("spark.serializer", "org.apache.spark.serializer.KyroSerializer");
			JavaSparkContext jsc = new JavaSparkContext(sparkConf);
			
			Configuration hbaseconf = HBaseConfiguration.create();
			// Change configuration file as per your server.
			hbaseconf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));
			hbaseconf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
			hbaseconf.set("hbase.client.retries.number", "10");
			hbaseconf.set(TableInputFormat.INPUT_TABLE, tableName);
			
			JavaPairRDD<ImmutableBytesWritable, Result> javaPairRdd = 
					jsc.newAPIHadoopRDD(hbaseconf, TableInputFormat.class, ImmutableBytesWritable.class,Result.class);
			if(!javaPairRdd.isEmpty()) {
				javaPairRdd.foreach(
						x -> System.out.println("Data from Hbase -> "+getStringData(x._2(),"default","topicname"))
						);
			}else {
				System.out.println("No data present");
			}
		jsc.stop();
		}
	}

	private static String getStringData(Result result, String cf, String name) {
		String data = Bytes.toString(result.getValue(Bytes.toBytes(cf), Bytes.toBytes(name)));
		return data;
	}

}
