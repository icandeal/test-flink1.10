package com.etiantian.bigdata.ds

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
/**
 * Created by yuchunfan on 2020/6/2.
 */
object TestS3 {
  def main(args: Array[String]): Unit = {
    val senv = StreamExecutionEnvironment.getExecutionEnvironment

    senv.readTextFile("s3a://aws-hadoop/test/data/city.csv").print()

    senv.execute()
  }
}
