package com.etiantian.bigdata.testV4

import java.util.Properties

import com.etiantian.bigdata.flink.graph.TablePoint
import com.etiantian.bigdata.flink.graph.context.MsgContext
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}
import org.apache.flink.types.Row
import org.json.JSONObject

/**
  * Created by yuchunfan on 2020/3/24.
  */
class ClassUserTable extends TablePoint{
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
      (json.get("uid").toString.toLong, json.get("cid").toString.toLong, json.get("utype").toString)
    })

    tenv.createTemporaryView("clazz_user", ds, 'uid, 'cid, 'utype)

    tenv.sqlQuery(
      """
        |SELECT
        | u.uid,
        | c.cid,
        | LAST_VALUE(u.name) name,
        | LAST_VALUE(u.age) age,
        | LAST_VALUE(c.cname) cname,
        | LAST_VALUE(cu.utype) utype
        |FROM uzer u, clazz c, clazz_user cu
        |WHERE
        | u.uid = cu.uid and c.cid = cu.cid
        |GROUP BY
        | u.uid, c.cid
        |""".stripMargin).toRetractStream[Row].map(x => {
      val isRetract = x._1
      val row = x._2
      val uid = row.getField(0)
      val cid = row.getField(1)
      val name = row.getField(2)
      val age = row.getField(3)
      val cname = row.getField(4)
      val utype = row.getField(5)
      new MsgContext((isRetract, uid, cid, name, age, cname, utype), new Properties())
    })
  }
}
