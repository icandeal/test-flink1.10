package com.etiantian.bigdata.sql

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.catalog.hive.HiveCatalog
import org.apache.flink.api.scala._
/**
 * Created by yuchunfan on 2020/3/6.
 */
object TestHive {
  def main(args: Array[String]): Unit = {
    val senv = StreamExecutionEnvironment.getExecutionEnvironment

    val environmentSettings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build()
    val tenv = StreamTableEnvironment.create(senv, environmentSettings)
    val name            = "myhive"
    val defaultDatabase = "default"
    val hiveConfDir     = "/etc/hive/conf" // a local path
    val version         = "2.3.4"

    val hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version)
    tenv.registerCatalog("myhive", hive)

    // set the HiveCatalog as the current catalog of the session
    tenv.useCatalog("myhive")

    val res = tenv.sqlQuery("select user_id, user_name from user_info_hdfs")
    res.printSchema()
    tenv.toRetractStream[(Int, String)](res).print()

    senv.execute()
  }
}
