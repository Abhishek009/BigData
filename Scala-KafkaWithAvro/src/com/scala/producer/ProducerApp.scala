package com.scala.producer

import com.domain.User
import com.scala.producer.KafkaProducer


object ProducerApp extends App {

  private val topic = "movies"

  val producer = new KafkaProducer()

  val user1 = User(1, "Dell", None)
  val user2 = User(2, "Lenovo", Some("lenovo@my.com"))

  producer.send(topic, List(user1, user2))

}