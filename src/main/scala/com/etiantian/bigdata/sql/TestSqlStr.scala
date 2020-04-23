package com.etiantian.bigdata.sql

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.types.Row

/**
 * Created by yuchunfan on 2020/4/8.
 */
object TestSqlStr {
  def main(args: Array[String]): Unit = {
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build()

    val tenv = StreamTableEnvironment.create(senv, settings)

    tenv.sqlUpdate(
      """
        |create table uzer (
        | uid bigint,
        | name varchar,
        | age int
        |) with (
        | 'connector.type' = 'kafka',
        | 'connector.version' = '0.11',
        | 'connector.topic' = 'test_user',
        | 'connector.startup-mode' = 'earliest-offset',
        | 'connector.properties.zookeeper.connect' = 'est124:2181,est125:2181/kafka',
        | 'connector.properties.bootstrap.servers' = 'est124:9092,est125:9092',
        | 'connector.properties.group.id' = 'flink-sql-test-202004081739',
        | 'format.type' = 'json',
        | 'format.derive-schema' = 'true',
        | 'update-mode' = 'append'
        |)
        |""".stripMargin)

    tenv.from("uzer").toAppendStream[Row].print()

    tenv.sqlUpdate(
      """
        |create table class_user (
        | uid bigint,
        | cid bigint,
        | utype varchar
        |) with (
        | 'connector.type' = 'kafka',
        | 'connector.version' = '0.11',
        | 'connector.topic' = 'test_class_user',
        | 'connector.startup-mode' = 'earliest-offset',
        | 'connector.properties.zookeeper.connect' = 'est124:2181,est125:2181/kafka',
        | 'connector.properties.bootstrap.servers' = 'est124:9092,est125:9092',
        | 'connector.properties.group.id' = 'flink-sql-test-202004101432',
        | 'format.type' = 'json',
        | 'format.derive-schema' = 'true',
        | 'update-mode' = 'append'
        |)
        |""".stripMargin)

    tenv.from("class_user").toAppendStream[Row].print()

    tenv.sqlUpdate(
      """
        |create table clazz_info (
        | cid bigint,
        | cname varchar
        |) with (
        | 'connector.type' = 'kafka',
        | 'connector.version' = '0.11',
        | 'connector.topic' = 'test_class',
        | 'connector.startup-mode' = 'earliest-offset',
        | 'connector.properties.zookeeper.connect' = 'est124:2181,est125:2181/kafka',
        | 'connector.properties.bootstrap.servers' = 'est124:9092,est125:9092',
        | 'connector.properties.group.id' = 'flink-sql-test-202004101432',
        | 'format.type' = 'json',
        | 'format.derive-schema' = 'true',
        | 'update-mode' = 'append'
        |)
        |""".stripMargin)

    tenv.from("clazz_info").toAppendStream[Row].print()

//    tenv.connect(
//      new Kafka()
//        .version("0.11")
//        .topic("test_class")
//        .property("zookeeper.connect", "t45:2181/kafka")
//        .property("bootstrap.servers", "t45:9092")
//        .property("group.id", "dev20200320")
//        .startFromEarliest()
//    ).withFormat(new Json).withSchema(
//      new Schema()
//        .field("cid", DataTypes.BIGINT())
//        .field("cname", DataTypes.STRING())
//    ).inAppendMode().createTemporaryTable("clazz")
//
//    tenv.connect(
//      new Kafka()
//        .version("0.11")
//        .topic("test_class_user")
//        .property("zookeeper.connect", "t45:2181/kafka")
//        .property("bootstrap.servers", "t45:9092")
//        .property("group.id", "dev20200320")
//        .startFromEarliest()
//    ).withFormat(new Json).withSchema(
//      new Schema()
//        .field("cid", DataTypes.BIGINT())
//        .field("uid", DataTypes.BIGINT())
//        .field("utype", DataTypes.STRING())
//    ).inAppendMode().createTemporaryTable("clazz_user")
//
//    tenv.sqlQuery(
//      """
//        |SELECT
//        | u.uid,
//        | c.cid,
//        | LAST_VALUE(u.name) name,
//        | LAST_VALUE(u.age) age,
//        | LAST_VALUE(c.cname) cname,
//        | LAST_VALUE(cu.utype) utype
//        |FROM uzer u, clazz c, clazz_user cu
//        |WHERE
//        | u.uid = cu.uid and c.cid = cu.cid
//        |GROUP BY
//        | u.uid, c.cid
//        |""".stripMargin).toRetractStream[Row].print

    senv.execute()
  }
}
