package com.etiantian.bigdata.graphic

import com.etiantian.bigdata.flink.graph.TablePoint
import com.etiantian.bigdata.flink.graph.context.MsgContext
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}

/**
 * Created by yuchunfan on 2020/4/20.
 */
class TableSinkPoint(esHosts: String) extends TablePoint{
  override def process(tenv: StreamTableEnvironment, dataStream: DataStream[MsgContext]): DataStream[MsgContext] = {

    tenv.createTemporaryView(
      "tp_task_performance",
      dataStream.map(_.getMsg.asInstanceOf[(String, Long, Long, Int, Int, Long, String, Long)]),
      'user_id, 'task_id, 'course_id, 'task_type, 'creteria_type, 'c_time, 'op, 'ts_ms, 'proctime.proctime
    )

    val t = tenv.sqlQuery(
      """
        |SELECT
        | user_id,
        | task_id,
        | course_id,
        | task_type,
        | creteria_type,
        | c_time,
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
         | 'connector.hosts' = '$esHosts',
         | 'connector.index' = 'test_performance_graphic',
         | 'connector.document-type' = 'info',
         | 'update-mode' = 'upsert',
         | 'format.type' = 'json'
         |)
         |""".stripMargin)

    val testPerformanceTable = tenv.sqlQuery(
      """
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

    testPerformanceTable.insertInto("test_performance")

//    tenv.createTemporaryView("tpf", testPerformanceTable)
//
//    tenv.sqlQuery(
//      """
//        |SELECT
//        | b.`ref` AS user_ref,
//        | LAST_VALUE(b.user_id),
//        | LAST_VALUE(b.ett_user_id) AS jid,
//        | LAST_VALUE(task_count),
//        | LAST_VALUE(course_count),
//        | LAST_VALUE(sum_task_type),
//        | LAST_VALUE(sum_creteria_type),
//        | LAST_VALUE(sum_time),
//        | LAST_VALUE(sum_create),
//        | LAST_VALUE(sum_other),
//        | MAX(max_proctime) AS m_time
//        |FROM tpf AS a
//        |INNER JOIN user_info
//        | FOR SYSTEM_TIME AS OF a.max_proctime AS b
//        | ON a.user_id = b.`ref`
//        |GROUP BY b.`ref`
//        |""".stripMargin).insertInto("test_result_flink")

    null
  }
}
