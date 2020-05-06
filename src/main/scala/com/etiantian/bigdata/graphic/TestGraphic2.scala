package com.etiantian.bigdata.graphic

import java.io.FileInputStream
import java.util.Properties

import com.etiantian.bigdata.JsonDeserializationSchema
import com.etiantian.bigdata.flink.graph.Graph
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.Path
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment

/**
 * Created by yuchunfan on 2020/4/20.
 */
object TestGraphic2 {

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


    val kafkaProp = new Properties()
    kafkaProp.setProperty("bootstrap.servers", properties.getProperty("bootstrap.servers"))
    kafkaProp.setProperty("auto.offset.reset", properties.getProperty("auto.offset.reset"))
    kafkaProp.setProperty("group.id", properties.getProperty("group.id"))

    val consumer011 = new FlinkKafkaConsumer011[String](
      "ip-10-1-10-31.cn-northwest-1.compute.internal.school.tp_task_performance",
      new JsonDeserializationSchema(),
      kafkaProp
    ).setStartFromEarliest
    val dataStream = senv.addSource(consumer011)

    val graph = Graph.draw(dataStream)
    graph.enableTableEnv(tenv)

    graph.addPoint("PreMap", new PreMap)
    graph.addPoint("JsonHandlerMap", new JsonHandlerMap)
    graph.addPoint("MysqlTablePoint", new MysqlTablePoint(
      properties.getProperty("connector.mysql.url"),
      properties.getProperty("connector.mysql.username"),
      properties.getProperty("connector.mysql.password")
    ))
    graph.addPoint("TableSinkPoint", new TableSinkPoint(properties.getProperty("connector.es.hosts")))
//    graph.addPoint("PrintEnd", new PrintEnd)

    graph.addEdge("PreMap", "JsonHandlerMap", _ != null)
    graph.addEdge("JsonHandlerMap", "TableSinkPoint", null)

    graph.finish()

    senv.execute()
  }
}
