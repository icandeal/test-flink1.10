package com.etiantian.bigdata.sql.jdbc

import org.apache.flink.api.java.io.jdbc.{JDBCOptions, JDBCTableSource}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, TableSchema}
import org.apache.flink.table.types.DataType
import org.apache.flink.types.Row

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._

/**
 * Created by yuchunfan on 2020/3/6.
 */
object TestMysql {
  def main(args: Array[String]): Unit = {
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    val setting = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tenv = StreamTableEnvironment.create(senv,setting)

    val taskMap = Map(
      "task_id" -> DataTypes.BIGINT(),
      "status" -> DataTypes.INT()
    )

    val allotMap = Map(
      "task_id" -> DataTypes.BIGINT(),
      "user_type" -> DataTypes.INT(),
      "user_type_id" -> DataTypes.BIGINT(),
      "b_time" -> DataTypes.TIMESTAMP(3),
      "e_time" -> DataTypes.TIMESTAMP(3)
    )

    val classUserMap = Map(
      "user_id" -> DataTypes.STRING(),
      "class_id" -> DataTypes.INT(),
      "relation_type" -> DataTypes.STRING()
    )

    val userMap = Map(
      "ref" -> DataTypes.STRING(),
      "user_id" -> DataTypes.INT()
    )

//    registTable(tenv, "tp_task_info", taskMap)
//
//    val task = tenv.from("tp_task_info")
//    tenv.toAppendStream[(Long, Int)](task).print()
//    registTable(tenv, "tp_task_allot_info", allotMap)
//    registTable(tenv, "j_class_user", classUserMap)

//    val taskTable = tenv.from("tp_task_info").renameColumns('task_id as 't_task_id)
//    val allotTable = tenv.from("tp_task_allot_info").renameColumns('task_id as 'a_task_id)
//    val classUserTable = tenv.from("j_class_user").renameColumns('user_id as 'j_ref)

    val stream = senv.socketTextStream("localhost", 9999).map(_.toInt)
    stream.print
    val table = tenv.fromDataStream(stream, 's_user_id, 'proctime.proctime)
    tenv.createTemporaryView("s_table", table)

//    val gradeMap = Map(
//      "grade_id" -> DataTypes.INT(),
//      "grade_name" -> DataTypes.STRING()
//    )
//    registTable(tenv, "grade_info", gradeMap)

//    val resTable = tenv.sqlQuery(
//      "select * from s_table s left join grade_info FOR SYSTEM_TIME AS OF s.proctime as u on s.s_grade_id = u.grade_id"
//    )
//
//    tenv.toRetractStream[Row](resTable).print()

    registTable(tenv, "user_info", userMap)
//    val userTable = tenv.from("user_info").renameColumns('ref as 'u_ref, 'user_id as 'u_user_id)
//    tenv.createTemporaryView("u_table", userTable)
//
//

    val resTable = tenv.sqlQuery(
      "select s.s_user_id,u.`ref` from s_table s left join user_info FOR SYSTEM_TIME AS OF s.proctime as u on s.s_user_id = u.user_id")

    tenv.toRetractStream[Row](resTable).print()

    senv.execute()
  }

  def registTable(tenv: StreamTableEnvironment, tableName: String, columnMap: Map[String, DataType]): Unit = {
    val jdbcOptions = JDBCOptions.builder()
      .setDBUrl("jdbc:mysql://gw4.bj.etiantian.net:12262/school?autoReconnect=true&failOverReadOnly=false&useSSL=false")
      .setUsername("schu")
      .setPassword("test")
      .setTableName(tableName)
      .build()

    val schema = TableSchema.builder().fields(columnMap.keys.toArray[String], columnMap.values.toArray[DataType]).build()

    val jdbcTableScource = JDBCTableSource.builder().setOptions(jdbcOptions).setSchema(schema).build()
    tenv.registerTableSource(tableName, jdbcTableScource)
  }
}
