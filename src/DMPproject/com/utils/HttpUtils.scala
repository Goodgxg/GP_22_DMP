package com.utils

import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.util.EntityUtils

object HttpUtils {
  def get(url:String): String ={
    val client: CloseableHttpClient = HttpClients.createDefault()
    val get = new HttpGet(url)

    val response: CloseableHttpResponse = client.execute((get))

    EntityUtils.toString(response.getEntity,"UTF-8")

  }

}
