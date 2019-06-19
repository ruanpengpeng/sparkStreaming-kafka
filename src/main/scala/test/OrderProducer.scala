package test

import java.io.FileInputStream
import java.util.Properties

import com.alibaba.fastjson.JSONObject
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

import scala.util.Random

object OrderProducer {
  def main(args: Array[String]): Unit = {
    val topic: String = "order"
    val properties= new Properties()
    //文件要放到resource文件夹下
    val path = Thread.currentThread().getContextClassLoader.getResource("producer.properties").getPath
    properties.load(new FileInputStream(path))
    val kafkaConfig =new ProducerConfig(properties)
    val producer=new Producer[String,String](kafkaConfig)
    while(true){
      val id =Random.nextInt(10)
      val event = new JSONObject()
      event.put("id",id)
      event.put("price",Random.nextInt(10000))
      producer.send(new KeyedMessage[String,String](topic,event.toString))
      println(topic)
      println("Message sent:"+ event.toString)
      Thread.sleep(Random.nextInt(1000))
    }
  }
}
