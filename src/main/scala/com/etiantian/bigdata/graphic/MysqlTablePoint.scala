package com.etiantian.bigdata.graphic

import com.etiantian.bigdata.flink.graph.TablePoint
import com.etiantian.bigdata.flink.graph.context.MsgContext
import org.apache.flink.api.java.io.jdbc.{JDBCOptions, JDBCTableSource}
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.{DataTypes, TableSchema}

/**
 * Created by yuchunfan on 2020/4/22.
 */
class MysqlTablePoint(url: String, username: String, password: String) extends TablePoint{

  override def process(tenv: StreamTableEnvironment, dataStream: DataStream[MsgContext]): DataStream[MsgContext] = {
    val jdbcOptions = JDBCOptions.builder()
        .setDBUrl(url)
        .setUsername(username)
        .setPassword(password)
        .setTableName("user_info")
        .build()

    val fieldMap = Map(
      "ref" -> DataTypes.STRING(),
      "user_id" -> DataTypes.BIGINT(),
      "ett_user_id" -> DataTypes.BIGINT()
    )

    val tableSchema = TableSchema.builder()
        .fields(fieldMap.keys.toArray, fieldMap.values.toArray)
        .build()
    val jdbcTableSource = JDBCTableSource.builder()
        .setOptions(jdbcOptions)
        .setSchema(tableSchema)
        .build()

    tenv.createTemporaryView("user_info", tenv.fromTableSource(jdbcTableSource))

    tenv.sqlUpdate(
      s"""
        |CREATE TABLE test_result_flink (
        | user_ref STRING,
        | user_id BIGINT,
        | jid BIGINT,
        | task_count BIGINT,
        | course_count BIGINT,
        | sum_task_type INT,
        | sum_creteria_type INT,
        | sum_time BIGINT,
        | sum_create INT,
        | sum_other INT,
        | m_time TIMESTAMP(3)
        |) WITH (
        | 'connector.type' = 'jdbc',
        | 'connector.table' = 'test_result_flink',
        | 'connector.url' = 'jdbc:mysql://10.1.1.176:3306/test?autoReconnect=true&failOverReadOnly=false&useSSL=false',
        | 'connector.driver' = 'com.mysql.jdbc.Driver',
        | 'connector.username' = 'hadoop',
        | 'connector.password' = 'etiantian2018!',
        | 'connector.write.flush.max-rows' = '10',
        | 'connector.write.flush.interval' = '100'
        |)
        |""".stripMargin)

    null
  }
}
