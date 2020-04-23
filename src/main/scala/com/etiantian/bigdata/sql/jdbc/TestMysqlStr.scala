package com.etiantian.bigdata.sql.jdbc

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/**
 * Created by yuchunfan on 2020/4/22.
 */
object TestMysqlStr {
  def main(args: Array[String]): Unit = {
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance()
      .inStreamingMode()
      .useBlinkPlanner()
      .build()
    val tenv = StreamTableEnvironment.create(senv, settings)

    tenv.sqlUpdate(
      """
        |CREATE TABLE user1 (
        | user_id INT,
        | user_name STRING,
        | user_type STRING,
        | user_comment STRING,
        | c_time TIMESTAMP(3),
        | m_time TIMESTAMP(3)
        |) WITH (
        | 'connector.type' = 'jdbc',
        | 'connector.url' = 'jdbc:mysql://t45:3306/test?autoReconnect=true&failOverReadOnly=false&useSSL=false',
        | 'connector.table' = 'user1',
        | 'connector.driver' = 'com.mysql.jdbc.Driver',
        | 'connector.username' = 'test',
        | 'connector.password' = 'Etiantian2018!',
        | 'connector.read.fetch-size' = '100',
        | 'connector.write.flush.max-rows' = '1',
        | 'connector.write.flush.interval' = '100'
        |)
        |""".stripMargin)

    tenv.sqlUpdate(
      """
        |CREATE TABLE user2 (
        | `ref` INT,
        | `name` STRING,
        | `comment` STRING
        |) WITH (
        | 'connector.type' = 'jdbc',
        | 'connector.url' = 'jdbc:mysql://t45:3306/test?autoReconnect=true&failOverReadOnly=false&useSSL=false',
        | 'connector.table' = 'user2',
        | 'connector.driver' = 'com.mysql.jdbc.Driver',
        | 'connector.username' = 'test',
        | 'connector.password' = 'Etiantian2018!',
        | 'connector.read.fetch-size' = '100'
        |)
        |""".stripMargin)

    tenv.sqlUpdate(
      """
        |CREATE TABLE k_user (
        | `rrr` INT,
        | `type` STRING,
        | `proctime` AS PROCTIME()
        |) WITH (
        | 'connector.type' = 'kafka',
        | 'connector.version' = '0.11',
        | 'connector.topic' = 'k_user',
        | 'connector.startup-mode' = 'latest-offset',
        | 'connector.properties.zookeeper.connect' = 't45:2181/kafka',
        | 'connector.properties.bootstrap.servers' = 't45:9092',
        | 'connector.properties.group.id' = 'test-t45',
        | 'format.type' = 'json',
        | 'format.derive-schema' = 'true',
        | 'update-mode' = 'append'
        |)
        |""".stripMargin)

    val t = tenv.sqlQuery(
      """
        |SELECT
        | a.`rrr` AS user_id,
        | LAST_VALUE(a.`type`) AS user_type,
        | LAST_VALUE(b.`name`) AS user_name,
        | LAST_VALUE(b.`comment`) AS user_comment,
        | MIN(a.proctime) c_time,
        | MAX(a.proctime) m_time
        |FROM k_user a
        |LEFT JOIN user2
        | FOR SYSTEM_TIME AS OF a.proctime AS b
        | ON a.`rrr` = b.`ref`
        |GROUP BY a.rrr
        |""".stripMargin)

    tenv.createTemporaryView("t_user1", t)
    t.toRetractStream[Row].print()
    t.select('user_id, 'user_name, 'user_type, 'user_comment, 'c_time, 'm_time).insertInto("user1")
//    tenv.sqlUpdate(
//      """
//        |INSERT INTO user1
//        |SELECT
//        | user_id,
//        | user_name,
//        | user_type,
//        | user_comment,
//        | c_time,
//        | m_time
//        | FROM t_user1
//        |""".stripMargin)

    senv.execute()

  }
}
