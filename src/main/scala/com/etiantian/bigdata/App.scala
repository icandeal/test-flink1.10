package com.etiantian.bigdata

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.json.JSONObject

import scala.collection.convert.wrapAsJava._
/**
 * Hello world!
 *
 */
object App {
  def main(args: Array[String]): Unit = {
    val senv = StreamExecutionEnvironment.getExecutionEnvironment

    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()

    val tableEnv = StreamTableEnvironment.create(senv,settings)

    val p = new Properties()
    p.setProperty("bootstrap.servers", "est221:9092,est222:9092,est223:9092")
//    p.setProperty("bootstrap.servers", "est221:9092,est222:9092,est223:9092")
    //    p.setProperty("bootstrap.servers", "52.82.47.234:9093,52.83.95.66:9093")
    p.setProperty("group.id", "test20200218")
    p.setProperty("enable.auto.commit", "true")
    p.setProperty("auto.offset.reset", "latest")

    val consumer = new FlinkKafkaConsumer011[String](
      List("aixueOnline"),
      new SimpleStringSchema(),
      p
    ).setStartFromLatest()
//    senv.addSource(consumer).name("source").filter(_.length > 10000).name("filttter").print()
    senv.addSource(consumer).map(x => {
      val json = new JSONObject(x)
      val url = if (json.has("url")) json.get("url") else ""
      val project_name = if (json.has("project_name")) json.get("project_name") else ""
      val model_name = if (json.has("model_name")) json.get("model_name") else ""
      val cost_time =if (json.has("cost_time")) json.get("cost_time") else ""
      val c_time = if (json.has("c_time")) json.get("c_time") else ""
      val jid = if (json.has("jid")) json.get("jid").toString else ""
      val user_id = if (json.has("user_id")) json.get("user_id").toString else ""
      (jid, user_id, c_time, url, project_name, model_name, cost_time)
    })
    //    val config = new util.HashMap[String, String]
    //    config.put("cluster.name", "espro")
    //    config.put("bulk.flush.max.actions", "1000")
    //
    //    val addressList = "10.1.10.124,10.1.10.125".split(",").map(x =>{
    //      new InetSocketAddress(InetAddress.getByName(x), 9300)
    //    }).toList

    //    senv.addSource(consumer).addSink(new ElasticsearchSink(config, addressList, new ElasticsearchSinkFunction[String] {
    //      override def process(t: String, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer) = {
    //        val jsonXContentBuilder = JsonXContent.contentBuilder().startObject()
    //        jsonXContentBuilder.field("message", t)
    //        jsonXContentBuilder.endObject()
    //        val indexName = "test_action_logs"
    //        val index = new IndexRequest().index(
    //          indexName
    //        ).`type`(
    //          "logs"
    //        ).source(jsonXContentBuilder)
    //        requestIndexer.add(index)
    //      }
    //    }))
    senv.execute()
  }
}
