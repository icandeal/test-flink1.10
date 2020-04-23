package com.etiantian.bigdata

import java.io.FileInputStream
import java.util.Properties

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.core.fs.Path
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup

/**
 * Created by yuchunfan on 2020/4/10.
 */
object TestTaskPerformance {

  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    properties.load(new FileInputStream(args(0)))

    val senv = StreamExecutionEnvironment.getExecutionEnvironment

    val checkPointPath = new Path(properties.getProperty("fs.backend.dir"))
    val fsStateBackend = new FsStateBackend(checkPointPath)
    senv.setStateBackend(fsStateBackend)
    senv.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    senv.enableCheckpointing(properties.getProperty("checkpoint.interval.ms").toInt)
    senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val settings = EnvironmentSettings.newInstance()
      .inStreamingMode()
      .useBlinkPlanner()
      .build()
    val tenv = StreamTableEnvironment.create(senv, settings)

    tenv.sqlUpdate(
      s"""
         |CREATE TABLE tp_task_performance (
         | before ROW(USER_ID STRING, TASK_ID BIGINT, COURSE_ID BIGINT, TASK_TYPE INT, CRETERIA_TYPE INT, C_TIME BIGINT),
         | after ROW(USER_ID STRING, TASK_ID BIGINT, COURSE_ID BIGINT, TASK_TYPE INT, CRETERIA_TYPE INT, C_TIME BIGINT),
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
         | 'format.derive-schema' = 'true',
         | 'update-mode' = 'append'
         |)
         |""".stripMargin)

    val t = tenv.sqlQuery(
      """
        |SELECT
        | COALESCE(after.USER_ID, before.USER_ID) user_id,
        | COALESCE(after.TASK_ID, before.TASK_ID) task_id,
        | COALESCE(after.COURSE_ID, before.COURSE_ID) course_id,
        | COALESCE(after.TASK_TYPE, before.TASK_TYPE) task_type,
        | COALESCE(after.CRETERIA_TYPE, before.CRETERIA_TYPE) creteria_type,
        | COALESCE(after.C_TIME, before.C_TIME) c_time,
        | op,
        | TO_TIMESTAMP(FROM_UNIXTIME(ts_ms/1000)) ts_ms,
        | proctime
        | FROM tp_task_performance
        |""".stripMargin)

    tenv.createTemporaryView("performance", t)

    tenv.sqlUpdate(
      s"""
         |CREATE TABLE test_performance (
         | user_id STRING,
         | task_count BIGINT,
         | course_count BIGINT,
         | sum_task_type INT,
         | sum_creteria_type INT,
         | sum_time BIGINT,
         | sum_create INT,
         | sum_other INT,
         | max_proctime TIMESTAMP(3)
         |) WITH (
         | 'connector.type' = 'elasticsearch',
         | 'connector.version' = '6',
         | 'connector.hosts' = '${properties.getProperty("connector.es.hosts")}',
         | 'connector.index' = 'test_performance',
         | 'connector.document-type' = 'info',
         | 'update-mode' = 'upsert',
         | 'format.type' = 'json'
         |)
         |""".stripMargin)

    tenv.sqlUpdate(
      """
        |INSERT INTO test_performance
        |SELECT
        | user_id,
        | COUNT(DISTINCT task_id) task_count,
        | COUNT(DISTINCT course_id) course_count,
        | SUM(task_type) sum_task_type,
        | SUM(creteria_type) sum_creteria_type,
        | SUM(c_time) sum_time,
        | SUM(IF(op = 'c',1, 0)) sum_create,
        | SUM(IF(op <> 'c',1, 0)) sum_other,
        | MAX(proctime) max_proctime
        |FROM performance
        |WHERE
        | proctime BETWEEN proctime - INTERVAL '5' MINUTE AND proctime
        |GROUP BY user_id
        |""".stripMargin)

    senv.execute()
  }
}
