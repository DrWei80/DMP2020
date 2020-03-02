package com.utils

import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

object HttpsUtil {
  /**
    * GET 请求
    * @param urlStr
    */
  def get(urlStr: String) = {
    val client = HttpClients.createDefault()
    val httpGet = new HttpGet(urlStr)
    // 发送请求
    val response = client.execute(httpGet)
    // 做编码集限定
    EntityUtils.toString(response.getEntity,"UTF-8")
  }

}
