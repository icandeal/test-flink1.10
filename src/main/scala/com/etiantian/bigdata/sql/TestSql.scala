package com.etiantian.bigdata.sql

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import scala.collection.convert.wrapAsJava._
/**
 * Created by yuchunfan on 2020/3/3.
 */
object TestSql {

  case class UserInfo(uid: String, age: Int, sex: Int)
  case class ClassInfo(cid: String, grade: Int, subject: Int)
  case class JClassUser(uid: String, cid: String, role: Int)


  def main(args: Array[String]): Unit = {
    val senv = StreamExecutionEnvironment.getExecutionEnvironment

    val environmentSettings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build()
    val tenv = StreamTableEnvironment.create(senv, environmentSettings)

//    tenv.useDatabase("tsql")
//    tenv.useCatalog("./tsql-catalog")

    val prop = new Properties()

    prop.setProperty("bootstrap.servers", "t45:9092")
    //    p.setProperty("bootstrap.servers", "est221:9092,est222:9092,est223:9092")
    //    p.setProperty("bootstrap.servers", "52.82.47.234:9093,52.83.95.66:9093")
    prop.setProperty("group.id", "test20200218")
    prop.setProperty("enable.auto.commit", "true")
    prop.setProperty("auto.offset.reset", "latest")

    val userInfo = new FlinkKafkaConsumer011[String](
      List("test_user_info"),
      new SimpleStringSchema,
      prop
    ).setStartFromGroupOffsets()
    val classInfo = new FlinkKafkaConsumer011[String](
      List("test_class_info"),
      new SimpleStringSchema,
      prop
    ).setStartFromGroupOffsets()
    val jClassUser = new FlinkKafkaConsumer011[String](
      List("test_j_class_user"),
      new SimpleStringSchema,
      prop
    ).setStartFromGroupOffsets()
    senv.addSource(userInfo).print()
    val userDS = senv.addSource(userInfo).map(x => {
      val array = x.split(",")
      UserInfo(array(0),array(1).toInt,array(2).toInt)
    })
    val classDS = senv.addSource(classInfo).map(x => {
      val array = x.split(",")
//      ClassInfo(array(0),array(1).toInt,array(2).toInt)
      (array(0),array(1).toInt,array(2).toInt)
    })
    val jcuDS = senv.addSource(jClassUser).map(x => {
      val array = x.split(",")
      JClassUser(array(0),array(1),array(2).toInt)
    })

//    userDS.print
//    classDS.print
//    jcuDS.print

    val userTable = tenv.fromDataStream(userDS)
    val classTable = tenv.fromDataStream(classDS, 'cid, 'grade, 'subject, 'ptime.proctime)
    val jcuTable = tenv.fromDataStream(jcuDS)

    tenv.toRetractStream[UserInfo](userTable).print
    tenv.toRetractStream[Row](classTable).print
    tenv.toRetractStream[Row](jcuTable).print

    senv.execute()
  }
}
