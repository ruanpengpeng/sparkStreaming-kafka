package test

import java.io.FileInputStream
import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import utils.RedisClient

import scala.collection.JavaConversions._

object OrderConsumer {
  //Redis配置
  val dbIndex = 0
  //每件商品总销售额
  val orderTotalKey = "app::order::total"
  //每件商品上一分钟销售额
  val oneMinTotalKey = "app::order::product"
  //总销售额
  val totalKey = "app::order::all"
  def main(args: Array[String]): Unit = {
    val conf =new SparkConf().setMaster("local[2]").setAppName("SparkStreamingTest")
    val ssc = new StreamingContext(conf,Seconds(1))
    val properties = new Properties()
    val path = getClass.getClassLoader.getResource("consumer.properties").getPath
    properties.load(new FileInputStream(path))
    val topics:scala.Predef.Set[String] =Set("order")
    val kafkaParam: Iterator[(String, String)] =
    for (x <- properties.propertyNames())yield
      (x.toString,properties.getProperty(x.toString))

    val kafkaStream=  KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,kafkaParam.toMap,topics)

    val events = kafkaStream.flatMap(line => Some(JSON.parseObject(line._2)))

    val orders= events.map(x => (x.getString("id"), x.getLong("price"))).groupByKey().map(x => (x._1, x._2.size, x._2.reduceLeft(_ + _)))

    orders.foreachRDD(x =>
      x.foreachPartition(partition =>
        partition.foreach(x => {
          println("id=" + x._1 + " count=" + x._2 + " price=" + x._3)
          //保存到Redis中
          val jedis = RedisClient.pool.getResource
          jedis.select(dbIndex)
          //每个商品销售额累加
          jedis.hincrBy(orderTotalKey, x._1, x._3)
          //上一分钟第每个商品销售额
          jedis.hset(oneMinTotalKey, x._1.toString, x._3.toString)
          //总销售额累加
          jedis.incrBy(totalKey, x._3)
          jedis.close()
        })
      ))


    ssc.start()
    ssc.awaitTermination()
  }
}
