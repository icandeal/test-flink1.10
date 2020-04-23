package com.etiantian.bigdata.sql.hbase

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings}
import org.apache.flink.table.descriptors.{HBase, Rowtime, Schema}
import org.apache.flink.types.Row
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
/**
 * Created by yuchunfan on 2020/4/7.
 */
object TestHBase {
  def main(args: Array[String]): Unit = {
    val senv = StreamExecutionEnvironment.getExecutionEnvironment

    val settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build()

    val tenv = StreamTableEnvironment.create(senv, settings)

    tenv.sqlUpdate(
      """
        |CREATE TABLE delete_user_task_profile (
        | rowkey String,
        | info Row(user_id VARCHAR, task_id VARCHAR, class_id VARCHAR, group_id VARCHAR, course_id VARCHAR)
        |) WITH (
        | 'connector.type' = 'hbase',
        | 'connector.version' = '1.4.3',
        | 'connector.table-name' = 'delete_user_task_profile',
        | 'connector.zookeeper.quorum' = 'test-hadoop1:2181,test-hadoop2:2181,test-hadoop3:2181',
        | 'connector.zookeeper.znode.parent' = '/hbase',
        | 'connector.write.buffer-flush.max-size' = '10mb',
        | 'connector.write.buffer-flush.max-rows' = '1000',
        | 'connector.write.buffer-flush.interval' = '20s'
        | )
        |""".stripMargin)
    tenv.from("delete_user_task_profile").toRetractStream[Row].print()

    tenv.connect(
      new HBase()
        .version("1.4.3")
        .tableName("delete_user_task_profile")
        .zookeeperQuorum("test-hadoop1:2181,test-hadoop2:2181,test-hadoop3:2181")
        .zookeeperNodeParent("/hbase")
        .writeBufferFlushMaxRows(1000)
        .writeBufferFlushMaxSize("10mb")
        .writeBufferFlushInterval("2s")
    ).withSchema(
      new Schema()
        .field("rowkey", DataTypes.STRING()).rowtime(new Rowtime().watermarksPeriodicAscending())
        .field("info", DataTypes.ROW(
          DataTypes.FIELD("user_id", DataTypes.STRING()),
          DataTypes.FIELD("task_id", DataTypes.STRING()),
          DataTypes.FIELD("group_id", DataTypes.STRING()),
          DataTypes.FIELD("class_id", DataTypes.STRING()),
          DataTypes.FIELD("course_id", DataTypes.STRING())
        ))
    ).createTemporaryTable("delete_user_task_profile")

    tenv.from("delete_user_task_profile").toAppendStream[Row].print()

    senv.execute()
  }
}
