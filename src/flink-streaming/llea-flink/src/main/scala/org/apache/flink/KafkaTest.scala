import java.util.Properties

import org.apache.flink.api.scala._
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09
//import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.RedisSinkHeff
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

//import utils.{ConfigurationManager}
//import src.main.scala.utils.{LleaRedisMapper}

/////////////////////////////////////////////////////////////////////////
////class LleaRedisMapper extends RedisMapper[(String, String)]{
//class LleaRedisMapper extends RedisMapper[String]{
//  override def getCommandDescription: RedisCommandDescription = {
//    new RedisCommandDescription(RedisCommand.HSET, "HASH_NAME")
//    // new RedisCommandDescription(RedisCommand.HSET, "pipeline")
//  }
//  //override def getKeyFromData(data: (String, String)): String = ("asdf", "qwert)" //data._1
//  //override def getValueFromData(data: (String, String)): String = "123" //data._2
//  override def getKeyFromData(data: (String)): String = "qwer" //data._1
//  override def getValueFromData(data: (String)): String = data //data._2
//}
/////////////////////////////////////////////////////////////////////////

// TODO: move class definition to utils
class LleaRedisMapper extends RedisMapper[String]{
  override def getCommandDescription: RedisCommandDescription = {
    //new RedisCommandDescription(RedisCommand.SET)
    new RedisCommandDescription(RedisCommand.INCRBY)
  }
  override def getKeyFromData(data: (String)): String = "key_w_int_value" //data._1
  override def getValueFromData(data: (String)): String = "10" //data._2
}

object Main {
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val properties = new Properties()

    //// TODO: Use config files
    // use public dns of first worker, kafka will not be on flink master anymore in final config
    properties.setProperty("bootstrap.servers", "ec2-35-167-63-252.us-west-2.compute.amazonaws.com:9092")
    properties.setProperty("zookeeper.connect", "ec2-35-167-63-252.us-west-2.compute.amazonaws.com:2181")
    properties.setProperty("group.id", "org.apache.flink")

    ///// TODO: Use config files later
    // Configure Flink to Redis connector
    // val conf = new FlinkJedisPoolConfig.Builder().setHost(config.get("redis.host")).build()
    val conf = new FlinkJedisPoolConfig.Builder().setHost("52.25.10.79").build()

    val stream = env
      .addSource(new FlinkKafkaConsumer09[String]("pipeline", new SimpleStringSchema(), properties))
      // add algorithm here....
      // in the end: data must be kv._1
      // in the end: data must be kv._2
      //.addSink(new RedisSink[(String)](conf, new LleaRedisMapper))
      .addSink(new RedisSinkHeff[(String)](conf, new LleaRedisMapper))
      //.addSink(new FlinkKafkaProducer09[String]("localhost:9092", "flink-to-kafka", new SimpleStringSchema()))
     // 
     //.print 

    env.execute("Flink Kafka Example")
  }
}
