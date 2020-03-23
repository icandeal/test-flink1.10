package com.etiantian.bigdata.ds

import java.lang
import java.util.Properties

import org.apache.flink.api.common.functions.{CoGroupFunction, JoinFunction}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.json.JSONObject
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.convert.wrapAsScala._

/**
 * Created by yuchunfan on 2020/3/20.
 */
object TestJoin {
  def main(args: Array[String]): Unit = {
    val senv = StreamExecutionEnvironment.getExecutionEnvironment

    senv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "t45:9092")
    prop.setProperty("auto.offset.reset", "latest")

    val userSource = new FlinkKafkaConsumer011[String](
      "test_user",
      new SimpleStringSchema(),
      prop
    ).setStartFromEarliest()

    val classUserSource = new FlinkKafkaConsumer011[String](
      "test_class_user",
      new SimpleStringSchema(),
      prop
    ).setStartFromEarliest()

    val userDS = senv.addSource(userSource)
    val classUserDS = senv.addSource(classUserSource)

    userDS.print()
    classUserDS.print()

    val uds = userDS.map(x => new JSONObject(x)).map(x => (x.get("uid").toString, x.getString("name"), x.get("age").toString))
    val cuds = classUserDS.map(x => new JSONObject(x)).map(x => (x.get("uid").toString, x.get("cid").toString, x.get("utype").toString))
    uds.join(cuds).where(x => x._1).equalTo(x => x._1).window(
      TumblingProcessingTimeWindows.of(Time.seconds(10))
    ).apply(new JoinFunction[(String, String, String),(String,String,String),(String,String, String,String,String, String)] {
      override def join(first: (String, String, String), second: (String,String,String)): (String,String, String,String, String, String) = {
        ("JOIN", first._1, first._2, first._3, second._2, second._3)
      }
    }).print

    uds.coGroup(cuds).where(_._1).equalTo(_._1).window(
      TumblingProcessingTimeWindows.of(Time.seconds(10))
    ).apply(new CoGroupFunction[(String, String, String),(String,String,String),(String, String, String,String,String, String)] {
      override def coGroup(first: lang.Iterable[(String, String, String)], second: lang.Iterable[(String,String, String)], out: Collector[(String, String, String,String, String, String)]): Unit = {
        val d1 = first.toList
        val d2 = second.toList
        if (d1.nonEmpty) {
          if (d2.isEmpty) {
            d1.foreach(x => {
              out.collect(("CoGroup", x._1, x._2, x._3, null, null))
            })
          } else {
            d1.foreach(x => {
              d2.foreach(y => {
                out.collect("CoGroup", x._1, x._2, x._3, y._2, y._3)
              })
            })
          }
        }
      }
    }).print()

    senv.execute()
  }
}
