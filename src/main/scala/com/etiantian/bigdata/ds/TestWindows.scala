package com.etiantian.bigdata.ds

import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.convert.wrapAsJava._

/**
 * Created by yuchunfan on 2020/6/17.
 */
object TestWindows {
  def main(args: Array[String]): Unit = {

    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "localhost:9092")
    prop.setProperty("auto.offset.reset", "latest")

    val kafkaConsumer011 = new FlinkKafkaConsumer011[String](
      List("test2"),
      new SimpleStringSchema(),
      prop
    ).setStartFromEarliest()

    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    senv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val source = senv.addSource(kafkaConsumer011).map(x => {
      val a = x.split(",")
      (a(0), a(1).toInt)
    })

    // This is a shortcut for either `.window(TumblingEventTimeWindows.of(size))` or
    // `.window(TumblingProcessingTimeWindows.of(size))` depending on the time characteristic
    // set using [[StreamExecutionEnvironment.setStreamTimeCharacteristic()]]
    source.keyBy(_._1).timeWindow(
      Time.seconds(5)
    ).process(new ProcessWindowFunction[(String, Int),(String, Int, String), String, TimeWindow]{
      override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[(String, Int, String)]): Unit = {
        val sum = elements.map(_._2).reduce(_ + _)
        out.collect(key, sum, context.window.getStart.toString)
      }
    }).print()

    source.keyBy(_._1).window(
      TumblingProcessingTimeWindows.of(Time.seconds(5))
    ).reduce((x, y) => (x._1, x._2 + y._2)).print()



    source.keyBy(_._1).timeWindow(
      Time.seconds(5)
    ).aggregate(new MyAggregateFunction).print()


    source.keyBy(_._1).window(
      TumblingProcessingTimeWindows.of(Time.seconds(5))
    ).aggregate(new MyAggregateFunction, new ProcessWindowFunction[(String, Double), (String, String, Double), String, TimeWindow] {
      override def process(key: String, context: Context, elements: Iterable[(String, Double)], out: Collector[(String, String, Double)]): Unit = {
        out.collect((key, context.window.getStart.toString, elements.toList(0)._2))
      }
    }).print()

    senv.execute()

  }

  class MyAggregateFunction extends AggregateFunction[(String, Int), (String, Int, Int), (String, Double)]{
    override def createAccumulator(): (String, Int, Int) = (null, 0, 0)

    override def add(value: (String, Int), accumulator: (String, Int, Int)): (String, Int, Int) = {
      if (accumulator._1 == null)
        (value._1, value._2, 1)
      else
        (value._1,accumulator._2 + value._2, accumulator._3 + 1)
    }

    override def getResult(accumulator: (String, Int, Int)): (String, Double) = (accumulator._1, accumulator._2.toDouble / accumulator._3)

    override def merge(a: (String, Int, Int), b: (String, Int, Int)): (String, Int, Int) = (a._1, a._2 + b._2, a._3 + b._3)
  }
}
