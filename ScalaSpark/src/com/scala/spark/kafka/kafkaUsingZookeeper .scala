package com.scala.spark.kafka

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils
import java.util.Date
import java.text.SimpleDateFormat

/*
 * @author:Abhishek Singh
 * Functionality:
 * Read data from kafka and store it into a HDFS location. 
 * Offset are store in zookeeper.
 * 
 * */

object kafkaUsingZookeeper {
  
  
  def startStream(
  topic: String,
  zkQuorum: String,
  appname: String,
  freq: Int,groupId: String
  ):StreamingContext={
    
    val sparkConf = new SparkConf().setAppName(appname);
    val ssc = new StreamingContext(sparkConf,Seconds(freq));
    val sc = ssc.sparkContext
    //val topics = topic.toMap
    val topics = Map(topic->1);
    val message = KafkaUtils.createStream(ssc, zkQuorum, groupId, topics)
    val kafkaMessage = message.map(_._2);
    
    val dNow = new Date();
    val ft = new SimpleDateFormat ("dd_hh_mm");
    
    kafkaMessage.foreach(rdd => {
      rdd.foreach(f => println("Kafka Message"+f))
      rdd.coalesce(2).saveAsTextFile("/home/data/"+topic+"/"+ft.format(dNow))
    })
    
    sys.ShutdownHookThread{
      println("Closing stream");
      ssc.stop(true,true);
      println("Stream Closed");
    }
    
    ssc
  }
  
  def main(args: Array[String]){
    
    val topic = "practice";
    val zkQuorum = "sandbox.hortonworks.com:2181";
    val appname = "kafkaInsert";
    val freq = 10;
    val groupId="practice_group";
    val stream = startStream(topic,zkQuorum,appname,freq,groupId);
    stream.start();
    stream.awaitTermination();
    
  }
}