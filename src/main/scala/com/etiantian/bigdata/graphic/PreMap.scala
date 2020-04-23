package com.etiantian.bigdata.graphic

import com.etiantian.bigdata.flink.graph.MapPoint
import org.json.JSONObject

/**
 * Created by yuchunfan on 2020/4/20.
 */
//class PreMap extends MapPoint[String, (String, Long, Long, Int, Int, Long, String, Long)]{
class PreMap extends MapPoint[String, JSONObject]{
  override def process(t: String): JSONObject = {
    val json = new JSONObject(t)
    if (json.has("value") && json.get("value") != null) new JSONObject(json.getString("value")) else null
  }
}
