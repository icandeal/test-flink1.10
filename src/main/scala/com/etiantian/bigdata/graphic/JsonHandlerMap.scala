package com.etiantian.bigdata.graphic

import com.etiantian.bigdata.flink.graph.MapPoint
import org.json.JSONObject

/**
 * Created by yuchunfan on 2020/4/20.
 */
class JsonHandlerMap extends MapPoint[JSONObject, (String, Long, Long, Int, Int, Long, String, Long)]{

  override def process(t: JSONObject): (String, Long, Long, Int, Int, Long, String, Long) = {
    val before = if (t.has("before") && t.get("before") != null && !t.get("before").toString.equals("null"))
      t.getJSONObject("before")
    else
      new JSONObject()

    val after = if (t.has("after") && t.get("after") != null && !t.get("after").toString.equals("null"))
      t.getJSONObject("after")
    else
      new JSONObject()

    val ref = if (after.has("USER_ID")) after.get("USER_ID").toString else before.get("USER_ID").toString
    val taskId = if (after.has("TASK_ID")) after.get("TASK_ID").toString.toLong else before.get("TASK_ID").toString.toLong
    val courseId = if (after.has("COURSE_ID")) after.get("COURSE_ID").toString.toLong else before.get("COURSE_ID").toString.toLong
    val taskType = if (after.has("TASK_TYPE")) after.get("TASK_TYPE").toString.toInt else before.get("TASK_TYPE").toString.toInt
    val creteriaType = if (after.has("CRETERIA_TYPE")) after.get("CRETERIA_TYPE").toString.toInt else before.get("CRETERIA_TYPE").toString.toInt
    val cTime = if (after.has("C_TIME")) after.get("C_TIME").toString.toLong else before.get("C_TIME").toString.toLong
    val op = t.get("op").toString
    val tsMs = t.get("ts_ms").toString.toLong

    (ref, taskId, courseId, taskType, creteriaType, cTime, op, tsMs)
  }
}
