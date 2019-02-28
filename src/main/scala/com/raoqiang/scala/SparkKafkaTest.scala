package com.raoqiang.scala

import java.util.Properties

import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import java.util.concurrent.Future

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.spark.broadcast.Broadcast

object SparkKafkaTest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("*").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("").setMaster("local[*]")
    val streamingContext = new StreamingContext(conf, Seconds(5))
    // kafka参数
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "192.168.67.133:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    // 创建kafka流
    val topics = Array("HC00_APPLICATION_SUBMIT")
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

//    stream.map(record => record.value()).foreachRDD(rdd =>
//      if(!rdd.isEmpty()){
//        rdd.collect().foreach(println)
//      }
//    )

//    val a = stream.map(record => (record.key, record.value))
    val re = stream.map(record => (1)).reduce((x,y)=>x+y)
    re.print()

    val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerConfig = {
        val p = new Properties()
        p.setProperty("bootstrap.servers", "192.168.31.213:9092")
        p.setProperty("key.serializer", classOf[StringSerializer].getName)
        p.setProperty("value.serializer", classOf[StringSerializer].getName)
        p
      }
//      log.warn("kafka producer init done!")
      streamingContext.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }

//    stream.foreachRDD(rdd => {
//      if (!rdd.isEmpty) {
//        rdd.foreach(record => {
//          kafkaProducer.value.send("test-topic", "raoqiang", record.value())
//          // do something else
//        })
//      }
//    })
      re.foreachRDD(rdd => {
        if (!rdd.isEmpty) {
          rdd.foreach(record => {
            kafkaProducer.value.send("test-topic", "raoqiang", record.toString)
            // do something else
          })
        }
      })

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}



class KafkaSink[K, V](createProducer: () => KafkaProducer[K, V]) extends Serializable {
  /* This is the key idea that allows us to work around running into
     NotSerializableExceptions. */
  lazy val producer = createProducer()
  def send(topic: String, key: K, value: V): Future[RecordMetadata] =
    producer.send(new ProducerRecord[K, V](topic, key, value))
  def send(topic: String, value: V): Future[RecordMetadata] =
    producer.send(new ProducerRecord[K, V](topic, value))
}

object KafkaSink {
  import scala.collection.JavaConversions._
  def apply[K, V](config: Map[String, Object]): KafkaSink[K, V] = {
    val createProducerFunc = () => {
      val producer = new KafkaProducer[K, V](config)
      sys.addShutdownHook {
        // Ensure that, on executor JVM shutdown, the Kafka producer sends
        // any buffered messages to Kafka before shutting down.
        producer.close()
      }
      producer
    }
    new KafkaSink(createProducerFunc)
  }
  def apply[K, V](config: java.util.Properties): KafkaSink[K, V] = apply(config.toMap)
}