package com.etiantian.bigdata.sql.hbase

import java.io.FileInputStream
import java.util.Properties

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.{EttHBase, Json, Kafka, Schema}
import org.apache.flink.types.Row
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.table.api.scala._

/**
 * Created by yuchunfan on 2020/4/29.
 */
object TestHBaseRead {
  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    properties.load(new FileInputStream(args(0)))

    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    senv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    val settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build()

    val tenv = StreamTableEnvironment.create(senv, settings)

//    tenv.connect(
//      new Kafka()
//        .version("0.11")
//        .topic("uzer")
//        .property("zookeeper.connect", "est124:2181,est125:2181/kafka")
//        .property("bootstrap.servers", "est124:9092,est125:9092")
//        .property("group.id", "dev20200320")
//        .startFromEarliest()
//    ).withFormat(new Json()).withSchema(
//      new Schema()
//        .field("user_id", DataTypes.STRING())
//    ).inAppendMode().createTemporaryTable("uzer")

    tenv.sqlUpdate(
      s"""
        |CREATE TABLE uzer (
        | user_id STRING,
        | proctime AS PROCTIME()
        |) WITH (
        | 'connector.type' = 'kafka',
        | 'connector.version' = '0.11',
        | 'connector.topic' = 'uzer',
        | 'connector.startup-mode' = '${properties.getProperty("auto.offset.reset")}-offset',
        | 'connector.properties.zookeeper.connect' = '${properties.getProperty("zookeeper.connect")}',
        | 'connector.properties.bootstrap.servers' = '${properties.getProperty("bootstrap.servers")}',
        | 'connector.properties.group.id' = '${properties.getProperty("group.id")}',
        | 'format.type' = 'json',
        | 'format.derive-schema' = 'true'
        |)
        |""".stripMargin)


    // Flink SQL use DDL to connect HBase
    tenv.sqlUpdate(
      """
        |CREATE TABLE user_info_mysql_user_id (
        | rowkey STRING,
        | info Row(ett_user_id STRING, dc_school_id STRING)
        |) WITH (
        | 'connector.type' = 'etthbase',
        | 'connector.table-name' = 'user_info_mysql_user_id',
        | 'connector.zookeeper.quorum' = 'test-hadoop1:2181,test-hadoop2:2181,test-hadoop3:2181',
        | 'connector.zookeeper.znode.parent' = '/hbase',
        | 'connector.write.buffer-flush.max-size' = '10mb',
        | 'connector.write.buffer-flush.max-rows' = '1000',
        | 'connector.write.buffer-flush.interval' = '1s'
        | )
        |""".stripMargin)
    // Flink SQL use HBase Connector to connect hbase
//    tenv.connect(
//      new EttHBase()
//        .tableName("user_info_mysql_user_id")
//        .zookeeperQuorum("test-hadoop1:2181,test-hadoop2:2181,test-hadoop3:2181")
//        .zookeeperNodeParent("/hbase")
//        .writeBufferFlushMaxRows(1000)
//        .writeBufferFlushMaxSize("10mb")
//        .writeBufferFlushInterval("1s")
//    ).withSchema(
//      new Schema()
//        .field("rowkey", DataTypes.STRING())
//        .field("info", DataTypes.ROW(
//          DataTypes.FIELD("ett_user_id", DataTypes.STRING()),
//          DataTypes.FIELD("dc_school_id", DataTypes.STRING())
//        ))
//    ).createTemporaryTable("user_info_mysql_user_id")

    tenv.from("uzer").toAppendStream[Row].print()

    val t = tenv.sqlQuery(
      """
        |SELECT
        | a.user_id,
        | b.info.ett_user_id AS jid,
        | b.info.dc_school_id AS dc_school_id
        |FROM uzer a INNER JOIN user_info_mysql_user_id AS b
        | ON a.user_id=b.rowkey
        |""".stripMargin)

    t.toAppendStream[Row].print()

    senv.execute()
  }
}
