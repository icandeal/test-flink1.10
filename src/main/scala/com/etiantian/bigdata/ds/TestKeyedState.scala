package com.etiantian.bigdata.ds

import java.util.Properties

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.function.RichAllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * Created by yuchunfan on 2020/6/23.
 */
object TestKeyedState {
  def main(args: Array[String]): Unit = {
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    senv.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
    senv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "localhost:9092")
    prop.setProperty("auto.offset.reset", "earliest")

    val consumer = new FlinkKafkaConsumer011[String](
      "test-state",
      new SimpleStringSchema(),
      prop
    ).setStartFromEarliest()

    val source = senv.addSource[String](consumer)
    source.process(new ProcessFunction[String, (String, Int)] {
      override def processElement(value: String, ctx: ProcessFunction[String, (String, Int)]#Context,
                                  out: Collector[(String, Int)]): Unit = {
        val array = value.split(",")
        out.collect((array(0), array(1).toInt))
      }
    }).keyBy(0).map(new RichMapFunction[(String, Int), (String, Int)] {
      override def map(value: (String, Int)): (String, Int) = {
        value
      }
    }).print()

    senv.execute()
  }
}
