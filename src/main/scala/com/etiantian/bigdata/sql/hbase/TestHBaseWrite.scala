package com.etiantian.bigdata.sql.hbase

import java.io.FileInputStream
import java.util.Properties

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings}
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}
import org.apache.flink.table.descriptors.{EttHBase, Json, Kafka, Schema}
import org.apache.flink.types.Row
/**
 * Created by yuchunfan on 2020/4/7.
 */
object TestHBaseWrite {
  def main(args: Array[String]): Unit = {
    val senv = StreamExecutionEnvironment.getExecutionEnvironment

    val settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build()

    val tenv = StreamTableEnvironment.create(senv, settings)

    tenv.connect(
      new Kafka()
        .version("0.11")
        .topic("test_user")
        .property("zookeeper.connect", "est124:2181,est125:2181/kafka")
        .property("bootstrap.servers", "est124:9092,est125:9092")
        .property("group.id", "dev20200320")
        .startFromEarliest()
    ).withFormat(new Json()).withSchema(
      new Schema()
        .field("uid", DataTypes.BIGINT())
        .field("name", DataTypes.STRING())
        .field("age", DataTypes.INT())
    ).inAppendMode().createTemporaryTable("uzer")

    // Flink SQL use DDL to insert HBase
//    tenv.sqlUpdate(
//      """
//        |CREATE TABLE test_user (
//        | rowkey STRING,
//        | info Row(uid STRING, name STRING, age STRING)
//        |) WITH (
//        | 'connector.type' = 'hbase',
//        | 'connector.table-name' = 'test_user',
//        | 'connector.zookeeper.quorum' = 'test-hadoop1:2181,test-hadoop2:2181,test-hadoop3:2181',
//        | 'connector.zookeeper.znode.parent' = '/hbase',
//        | 'connector.write.buffer-flush.max-size' = '10mb',
//        | 'connector.write.buffer-flush.max-rows' = '1000',
//        | 'connector.write.buffer-flush.interval' = '1s'
//        | )
//        |""".stripMargin)

    tenv.connect(
      new EttHBase()
        .tableName("test_user")
        .zookeeperQuorum("test-hadoop1:2181,test-hadoop2:2181,test-hadoop3:2181")
        .zookeeperNodeParent("/hbase")
        .writeBufferFlushMaxRows(1000)
        .writeBufferFlushMaxSize("10mb")
        .writeBufferFlushInterval("1s")
    ).withSchema(
      new Schema()
        .field("rowkey", DataTypes.STRING())
        .field("info", DataTypes.ROW(
          DataTypes.FIELD("uid", DataTypes.STRING()),
          DataTypes.FIELD("name", DataTypes.STRING()),
          DataTypes.FIELD("age", DataTypes.STRING())
        ))
    ).createTemporaryTable("test_user")

    tenv.from("uzer").toAppendStream[Row].print()
    tenv.from("uzer").select(
      'uid + "_key" as 'rowkey,
      row('uid + "", 'name, 'age + "").as('info)
    ).insertInto("test_user")

    senv.execute()
  }
}
