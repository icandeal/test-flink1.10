package com.etiantian.bigdata.sql

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings}
import org.apache.flink.table.descriptors.{Json, Kafka, Schema}
import org.apache.flink.types.Row

import org.apache.flink.api.scala._
/**
 * Created by yuchunfan on 2020/3/20.
 */
object TestSqlTime {
  def main(args: Array[String]): Unit = {
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build()

    val tenv = StreamTableEnvironment.create(senv, settings)

    tenv.connect(
      new Kafka()
        .version("0.11")
        .topic("test_user")
        .property("zookeeper.connect", "t45:2181/kafka")
        .property("bootstrap.servers", "t45:9092")
        .property("group.id", "dev20200320")
        .startFromEarliest()
    ).withFormat(new Json()).withSchema(
      new Schema()
        .field("uid", DataTypes.BIGINT())
        .field("name", DataTypes.STRING())
        .field("age", DataTypes.INT())
    ).inAppendMode().createTemporaryTable("uzer")

    tenv.connect(
      new Kafka()
        .version("0.11")
        .topic("test_class")
        .property("zookeeper.connect", "t45:2181/kafka")
        .property("bootstrap.servers", "t45:9092")
        .property("group.id", "dev20200320")
        .startFromEarliest()
    ).withFormat(new Json).withSchema(
      new Schema()
        .field("cid", DataTypes.BIGINT())
        .field("cname", DataTypes.STRING())
    ).inAppendMode().createTemporaryTable("clazz")

    tenv.connect(
      new Kafka()
        .version("0.11")
        .topic("test_class_user")
        .property("zookeeper.connect", "t45:2181/kafka")
        .property("bootstrap.servers", "t45:9092")
        .property("group.id", "dev20200320")
        .startFromEarliest()
    ).withFormat(new Json).withSchema(
      new Schema()
        .field("cid", DataTypes.BIGINT())
        .field("uid", DataTypes.BIGINT())
        .field("utype", DataTypes.STRING())
    ).inAppendMode().createTemporaryTable("clazz_user")

    tenv.toRetractStream[Row](tenv.sqlQuery(
      """
        |SELECT
        | u.uid,
        | c.cid,
        | LAST_VALUE(u.name) name,
        | LAST_VALUE(u.age) age,
        | LAST_VALUE(c.cname) cname,
        | LAST_VALUE(cu.utype) utype
        |FROM uzer u, clazz c, clazz_user cu
        |WHERE
        | u.uid = cu.uid and c.cid = cu.cid
        |GROUP BY
        | u.uid, c.cid
        |""".stripMargin)).print

    senv.execute()
  }
}
