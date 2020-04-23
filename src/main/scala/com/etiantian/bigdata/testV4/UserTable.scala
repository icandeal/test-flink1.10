package com.etiantian.bigdata.testV4

import com.etiantian.bigdata.flink.graph.TablePoint
import com.etiantian.bigdata.flink.graph.context.MsgContext
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}
import org.json.JSONObject

/**
  * Created by yuchunfan on 2020/3/24.
  */
class UserTable extends TablePoint{
  /**
    * table API 操作
    *
    * @param dataStream 可以使用的输入流，如果是根节点，输入流为graph输入流（包装成MsgContext之后的）
    * @return 输出流，可以重新包装成MsgContext流，参与graph计算，也可以为null作为无输出点或结束点
    */
  override def process(tenv: StreamTableEnvironment, dataStream: DataStream[MsgContext]): DataStream[MsgContext] = {
    val ds = dataStream.map(x => {
      val tuple2 = x.getMsg.asInstanceOf[(String, String)]
      val json = new JSONObject(tuple2._2)
      (json.get("uid").toString.toLong, json.get("name").toString, json.get("age").toString.toInt)
    })

    tenv.createTemporaryView("uzer", ds, 'uid, 'name, 'age)
    null
  }
}
