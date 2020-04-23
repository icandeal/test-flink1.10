package com.etiantian.bigdata.sql

import java.sql.Timestamp

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.{Json, Kafka, Rowtime, Schema}
import org.apache.flink.types.Row
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._

/**
 * Created by yuchunfan on 2020/4/13.
 */
object TestSqlConnector {

  case class UserInfo(uid: Long, cid: Long, name: String, age: Int, cname: String, utype: String, ctime: Timestamp)

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

    tenv.connect(
      new Kafka()
        .version("0.11")
        .topic("test_class")
        .property("zookeeper.connect", "est124:2181,est125:2181/kafka")
        .property("bootstrap.servers", "est124:9092,est125:9092")
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
        .property("zookeeper.connect", "est124:2181,est125:2181/kafka")
        .property("bootstrap.servers", "est124:9092,est125:9092")
        .property("group.id", "dev20200320")
        .startFromEarliest()
    ).withFormat(new Json).withSchema(
      new Schema()
        .field("cid", DataTypes.BIGINT())
        .field("uid", DataTypes.BIGINT())
        .field("utype", DataTypes.STRING())
        .field("c_time", DataTypes.TIMESTAMP(3))
//        .proctime()       // it doesn't work
//        .rowtime(new Rowtime().timestampsFromField("c_time").watermarksPeriodicBounded(2000))     // it doesn't work
    ).inAppendMode().createTemporaryTable("clazz_user")

    tenv.from("clazz_user").printSchema()
    tenv.from("clazz_user").toAppendStream[Row].print()
//    tenv.sqlQuery(
//      """
//        |SELECT
//        | u.uid,
//        | c.cid,
//        | LAST_VALUE(u.name) name,
//        | LAST_VALUE(u.age) age,
//        | LAST_VALUE(c.cname) cname,
//        | LAST_VALUE(cu.utype) utype,
//        | TUMBLE_START(c_time, INTERVAL '10' MINUTE)
//        |FROM uzer u, clazz c, clazz_user cu
//        |WHERE
//        | u.uid = cu.uid and c.cid = cu.cid
//        |GROUP BY
//        | TUMBLE(c_time, INTERVAL '10' MINUTE),
//        | u.uid, c.cid
//        |""".stripMargin).toRetractStream[UserInfo].print

    senv.execute("TestSqlConnector")
  }
}
