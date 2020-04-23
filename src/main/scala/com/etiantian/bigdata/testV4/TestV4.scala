package com.etiantian.bigdata.testV4

import java.util.Properties

import com.etiantian.bigdata.JsonDeserializationSchema
import com.etiantian.bigdata.flink.graph.Graph
import com.etiantian.bigdata.flink.graph.end.PrintEnd
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}
import org.apache.flink.types.Row

import scala.collection.convert.wrapAsJava._

/**
  * Created by yuchunfan on 2020/3/24.
  */
object TestV4 {

  def main(args: Array[String]): Unit = {
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build()

    val tenv = StreamTableEnvironment.create(senv, settings)

    val kafkaProp = new Properties()
    kafkaProp.setProperty("bootstrap.servers", "t45:9092")
    kafkaProp.setProperty("group.id", "dev20200320")
    val kafkaConsumer011 = new FlinkKafkaConsumer011[String](
      List("test_user","test_class", "test_class_user"),
      new JsonDeserializationSchema,
      kafkaProp
    ).setStartFromEarliest()

    val dataStream = senv.addSource(kafkaConsumer011)

    val graph = Graph.draw(dataStream)
    graph.enableTableEnv(tenv)

    graph.addPoint("PreMap", new PreMap)
    graph.addPoint("UserTable", new UserTable)
    graph.addPoint("ClazzTable", new ClazzTable)
    graph.addPoint("ClassUserTable", new ClassUserTable)
    graph.addPoint("PrintEnd", new PrintEnd)

    graph.addEdge("PreMap", "UserTable", x=> {
      x.asInstanceOf[(String, String)]._1.equals("test_user")
    })
    graph.addEdge("PreMap", "ClazzTable", x=> {
      x.asInstanceOf[(String, String)]._1.equals("test_class")
    })
    graph.addEdge("PreMap", "ClassUserTable", x=> {
      x.asInstanceOf[(String, String)]._1.equals("test_class_user")
    })

    graph.addEdge("ClassUserTable", "PrintEnd", null)

    graph.finish()
    tenv.from("clazz").toAppendStream[Row].print()
    tenv.from("uzer").toAppendStream[Row].print()
    tenv.from("clazz_user").toAppendStream[Row].print()

    senv.execute()
  }
}
