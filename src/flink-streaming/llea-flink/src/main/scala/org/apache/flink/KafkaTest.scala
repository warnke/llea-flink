import java.util.Properties

import org.apache.flink.api.scala._
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09
// Redis Connector
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
// import org.apache.flink.streaming.connectors.kafka.api.KafkaSink

import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

//import utils.{ConfigurationManager}
//import src.main.scala.utils.{LleaRedisMapper}

//class LleaRedisMapper extends RedisMapper[(String, String)]{
class LleaRedisMapper extends RedisMapper[String]{
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET, "HASH_NAME")
    // new RedisCommandDescription(RedisCommand.HSET, "pipeline")
  }
  //override def getKeyFromData(data: (String, String)): String = ("asdf", "qwert)" //data._1
  //override def getValueFromData(data: (String, String)): String = "123" //data._2
  override def getKeyFromData(data: (String)): String = "qwer" //data._1
  override def getValueFromData(data: (String)): String = "asdf" //data._2
}

object Main {
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val properties = new Properties()
    // println("hello")
    // use public dns of first worker, it should be the one where kafka runs on
    properties.setProperty("bootstrap.servers", "ec2-35-167-63-252.us-west-2.compute.amazonaws.com:9092")
    properties.setProperty("zookeeper.connect", "ec2-35-167-63-252.us-west-2.compute.amazonaws.com:2181")
    properties.setProperty("group.id", "org.apache.flink")

    println("hello")

    // Configure Flink to Redis connector
    // val conf = new FlinkJedisPoolConfig.Builder().setHost(config.get("redis.host")).build()
    val conf = new FlinkJedisPoolConfig.Builder().setHost("52.25.10.79").build()

    val stream = env
      .addSource(new FlinkKafkaConsumer09[String]("pipeline", new SimpleStringSchema(), properties))
     //.addSink(new RedisSink[String](conf, new FlinkRedisMapper))
     //.addSink(new RedisSink[String](conf, new LleaRedisMapper))
     //.addSink(new RedisSink[(String, String)](conf, new LleaRedisMapper))
     //.addSink(new RedisSink[(String, String)](conf, new LleaRedisMapper))
     .addSink(new RedisSink[(String)](conf, new LleaRedisMapper))
     //.print <-works
     //.addSink(new FlinkKafkaProducer09[String]("localhost:9092", "flink-to-kafka", new SimpleStringSchema()))    <- works
     // stream.addSink(new FlinkKafkaProducer09[String]("ec2-52-33-229-60.us-west-2.compute.amazonaws.com:9092", "flink-to-rethinkdb", new SimpleStringSchema()))
     // val x =  stream.addSink(new KafkaSink("ec2-52-33-229-60.us-west-2.compute.amazonaws.com:9092", "flink-output", new MySimpleStringSchema))
     //  println("hello")

    env.execute("Flink Kafka Example")
  }
}
