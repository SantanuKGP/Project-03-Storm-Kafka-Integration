import org.apache.storm.task.{OutputCollector, TopologyContext}
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichBolt
import org.apache.storm.tuple.{Fields, Tuple, Values}
import org.json.JSONObject

import java.util

class ProcessingBolt extends BaseRichBolt{

  private var collector : OutputCollector =_
  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit =
    declarer.declare(new Fields("key","message"))

  override def prepare(topologyConf: util.Map[String, AnyRef],
                       context: TopologyContext,
                       collector: OutputCollector): Unit = this.collector= collector

  override def execute(input: Tuple): Unit ={
    val keyVal = input.getStringByField("key")
    val message = input.getStringByField("message")
    val json = new JSONObject(message)
    val firstName :String = json.get("firstName").toString
    val lastName :String = json.get("lastName").toString
    json.put("fullName", firstName +" "+ lastName)
    json.put("txnTimeStamp",java.time.LocalDateTime.now().toString)

    collector.emit(new Values(keyVal,json.toString))
    collector.ack(input)
  }
}
