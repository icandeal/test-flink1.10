package com.etiantian.bigdata.testV4

import com.etiantian.bigdata.flink.graph.MapPoint
import org.json.JSONObject

/**
  * Created by yuchunfan on 2020/3/24.
  */
class PreMap extends MapPoint[String, (String, String)]{
  override def process(t: String): (String, String) = {
    val json = new JSONObject(t)
    val topic = json.getString("topic")
    val value = json.getString("value")
    (topic, value)
  }
}
