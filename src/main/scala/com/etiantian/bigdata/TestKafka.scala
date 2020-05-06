package com.etiantian.bigdata

import java.io.FileInputStream
import java.util.Properties

import org.apache.flink.core.fs.Path
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/**
 * Created by yuchunfan on 2020/4/10.
 */
object TestKafka {

  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    properties.load(new FileInputStream(args(0)))

    val senv = StreamExecutionEnvironment.getExecutionEnvironment

    val settings = EnvironmentSettings.newInstance()
      .inStreamingMode()
      .useBlinkPlanner()
      .build()
    val tenv = StreamTableEnvironment.create(senv, settings)

    tenv.sqlUpdate(
      s"""
         |CREATE TABLE tp_task_performance (
         | before ROW(USER_ID STRING, TASK_ID BIGINT, COURSE_ID BIGINT, TASK_TYPE INT, CRETERIA_TYPE INT, C_TIME BIGINT),
         | after STRING,
         | op STRING,
         | ts_ms BIGINT,
         | proctime AS PROCTIME()
         |) WITH (
         | 'connector.type' = 'kafka',
         | 'connector.version' = '0.11',
         | 'connector.topic' = 'ip-10-1-10-31.cn-northwest-1.compute.internal.school.tp_task_performance',
         | 'connector.startup-mode' = '${properties.getProperty("auto.offset.reset")}-offset',
         | 'connector.properties.zookeeper.connect' = '${properties.getProperty("zookeeper.connect")}',
         | 'connector.properties.bootstrap.servers' = '${properties.getProperty("bootstrap.servers")}',
         | 'connector.properties.group.id' = '${properties.getProperty("group.id")}',
         | 'format.type' = 'json',
         | 'format.json-schema' = '
         |  {
         |    "type": "object",
         |    "properties": {
         |      "before": {
         |        "type": "object",
         |        "properties": {
         |          "USER_ID": {"type": "string"},
         |          "TASK_ID": {"type": "integer"},
         |          "COURSE_ID": {"type": "integer"},
         |          "TASK_TYPE": {"type": "integer"},
         |          "CRETERIA_TYPE": {"type": "integer"},
         |          "C_TIME": {"type": "integer"}
         |        }
         |      },
         |      "after": {"type": "string"},
         |      "op": {"type": "string"},
         |      "ts_ms": {"type": "integer"}
         |    }
         |  }
         | ',
         | 'update-mode' = 'append'
         |)
         |""".stripMargin)

    tenv.from("tp_task_performance").select('after).toAppendStream[Row].print()

    senv.execute()
  }
}
