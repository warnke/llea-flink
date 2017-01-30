import java.util.Properties

import org.apache.flink.api.scala._
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09
// don't ask brennan about this later
// import org.apache.flink.streaming.connectors.kafka.api.KafkaSink

object Main {
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val properties = new Properties()
    println("hello")
    // use public dns of first worker, it should be the one where kafka runs on
    properties.setProperty("bootstrap.servers", "ec2-35-167-63-252.us-west-2.compute.amazonaws.com:9092")
    properties.setProperty("zookeeper.connect", "ec2-35-167-63-252.us-west-2.compute.amazonaws.com:2181")
    properties.setProperty("group.id", "org.apache.flink")
    println("hello")
    val stream = env
      .addSource(new FlinkKafkaConsumer09[String]("pipeline", new SimpleStringSchema(), properties))
      .print
     // .addSink(new FlinkKafkaProducer09[String]("ec2-52-33-229-60.us-west-2.compute.amazonaws.com:9092", "flink-to-kafka", new SimpleStringSchema()))

   /* stream.addSink(new FlinkKafkaProducer09[String]("ec2-52-33-229-60.us-west-2.compute.amazonaws.com:9092", "flink-to-rethinkdb", new SimpleStringSchema()))
*/
/*    val x =  stream.addSink(new KafkaSink("ec2-52-33-229-60.us-west-2.compute.amazonaws.com:9092", "flink-output", new MySimpleStringSchema))
  */  println("hello")
    env.execute("Flink Kafka Example")
  }
}
