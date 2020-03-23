package com.etiantian.bigdata.sql

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset}
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import scala.collection.convert.wrapAsJava._

/**
 * Created by yuchunfan on 2020/3/3.
 */
object TestCurrency {
  def main(args: Array[String]): Unit = {
    val senv = StreamExecutionEnvironment.getExecutionEnvironment

    val environmentSettings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build()
    val tenv = StreamTableEnvironment.create(senv, environmentSettings)

    senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "t45:9092")
    prop.setProperty("group.id", "test20200218")

    val ordersConsumer = new FlinkKafkaConsumer011[String](
      List("Orders"),
      new SimpleStringSchema,
      prop
    ).setStartFromEarliest()

    val ratesConsumer = new FlinkKafkaConsumer011[String](
      List("Rates"),
      new SimpleStringSchema(),
      prop
    ).setStartFromEarliest()

    val ordersDS = senv.addSource(ordersConsumer).map(x => {
      val array = x.split(",")
      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
      (LocalDateTime.parse(array(0), formatter).toInstant(ZoneOffset.ofHours(8)).toEpochMilli, array(1).toInt, array(2))
    })
    val ratesDS = senv.addSource(ratesConsumer).map(x => {
      val array = x.split(",")
      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
      (LocalDateTime.parse(array(0), formatter).toInstant(ZoneOffset.ofHours(8)).toEpochMilli, array(2).toInt, array(1), "T")
    })

    val a = ordersDS.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(Long, Int, String)] {

      val maxOutOfOrderness = 3500L
      var watermark: Watermark = _
      var currentMaxTimestamp = 0L

      override def getCurrentWatermark: Watermark = {
        new Watermark(currentMaxTimestamp - maxOutOfOrderness)
      }

      override def extractTimestamp(element: (Long, Int, String), previousElementTimestamp: Long): Long = {
        val timestamp = element._1
        currentMaxTimestamp = math.max(timestamp, currentMaxTimestamp)
        timestamp
      }
    })

    val b = ratesDS.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(Long, Int, String, String)] {

      val maxOutOfOrderness = 3500L
      var watermark: Watermark = _
      var currentMaxTimestamp = 0L

      override def getCurrentWatermark: Watermark = {
        new Watermark(currentMaxTimestamp - maxOutOfOrderness)
      }

      override def extractTimestamp(element: (Long, Int, String, String), previousElementTimestamp: Long): Long = {
        val timestamp = element._1
        currentMaxTimestamp = math.max(timestamp, currentMaxTimestamp)
        timestamp
      }
    })


    val ordersTable = tenv.fromDataStream(a, 'o_rowtime.rowtime, 'o_amount, 'o_currency)
    val ratesHistoryTable = tenv.fromDataStream(b, 'r_rowtime.rowtime, 'r_rate, 'r_currency, 't)

    tenv.createTemporaryView("Orders", ordersTable)
    tenv.createTemporaryView("RatesHistory", ratesHistoryTable)

    val rates = ratesHistoryTable.createTemporalTableFunction("r_rowtime", "r_currency")
    tenv.registerFunction("rates", rates)


    val query = ordersTable.joinLateral(rates('o_rowtime), 'o_currency === 'r_currency).select('o_currency, ('o_amount * 'r_rate) as 'amount)

    query.printSchema()
    tenv.toRetractStream[Row](ordersTable).print()
    tenv.toRetractStream[Row](ratesHistoryTable).print()
    tenv.toRetractStream[Row](query).print()
    senv.execute()
  }
}
