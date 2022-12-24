import org.apache.storm.generated.{AlreadyAliveException, InvalidTopologyException}
import org.apache.storm.kafka.bolt.KafkaBolt
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector
import org.apache.storm.kafka.spout.KafkaSpoutConfig.Builder
import org.apache.storm.kafka.spout.{ FirstPollOffsetStrategy, KafkaSpout, KafkaSpoutConfig, SimpleRecordTranslator}
import org.apache.storm.{Config, StormSubmitter}
import org.apache.storm.topology.TopologyBuilder
import org.apache.storm.tuple.{Fields, Values}

import java.util.Properties
import scala.util.Random

class MyTopology(readFrom:String,writeTo:String, ID:String="01") {

  private val builder = new TopologyBuilder

  /**
   * Spout Building
   * */

  private val spoutProps = new Properties()
  spoutProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  spoutProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  spoutProps.put("group.id", s"consumer-group-$ID")

  private val translator = new SimpleRecordTranslator[String, String](record =>
    new Values(record.value(),record.key()),
    new Fields("message","key"))

  private val spoutBuilder = new Builder[String, String]("localhost:9092", readFrom)
    .setFirstPollOffsetStrategy(FirstPollOffsetStrategy.EARLIEST)
    .setProp(spoutProps)
    .setRecordTranslator(translator)
    .build()

 // private val spoutConfig = new KafkaSpoutConfig[String, String](spoutBuilder)

  builder.setSpout("spout",new KafkaSpout(spoutBuilder),1)


  /**
   * Bolt Builder
   * */

  private val boltProps = new Properties()
  boltProps.put("bootstrap.servers", "localhost:9092")
  boltProps.put("acks", "1")
  boltProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  boltProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  builder.setBolt("bolt-1", new ProcessingBolt,2)
    .setNumTasks(2)
    .shuffleGrouping("spout")

  builder.setBolt("bolt-2",
    new KafkaBolt[String,String]()
      .withTopicSelector(new DefaultTopicSelector(s"$writeTo"))
      .withProducerProperties(boltProps)
      .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper("key","message"))
    ,2)
    .setNumTasks(2)
    .shuffleGrouping("bolt-1")


  private val config = new Config
  config.setDebug(false)
  config.setNumWorkers(1)
  try
    StormSubmitter.submitTopology(s"$readFrom-to-$writeTo",config,builder.createTopology())
  catch{
    case alreadyAliveException: AlreadyAliveException => println(alreadyAliveException)
    case invalidTopologyException: InvalidTopologyException => println(invalidTopologyException)
  }


}
object MyTopology{
  def main(args: Array[String]): Unit = {
    val num = new Random().nextInt(100000000)
        args.length match {
          case 0 => new MyTopology("request_txn","response_txn",num.toString)
          case 2 => new MyTopology(args(0), args(1))
          case 3 => new MyTopology(args(0), args(1), args(2))
          case _ => println(
            """
              |Valid Input : [readFrom] [writeTo] [duration: Optional] [offset : Optional]
              |readFrom : The topic name from where you want to read
              |writeTo : The topic name where you want to write
              |duration : By default 100. After how much time (seconds) Kafka reader will timeout
              |offset : By Default It is "latest". you can change it to "earliest".
              |""".stripMargin)
        }
  }
}