package com.atguigu.utils

import java.util
import java.util.Objects

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index}

object MyESUtils {

  private val ES_HOST = "http://hadoop102"
  private val ES_HTTP_PORT = 9200
  private var factory: JestClientFactory = _

  //获取客户端
  def getClient: JestClient = {
    if (factory == null)
      build()
    factory.getObject
  }

  //建立连接
  private def build() = {
    factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_HOST + ":" + ES_HTTP_PORT)
      .multiThreaded(true)
      .maxTotalConnection(10000)
      .connTimeout(10000)
      .readTimeout(10000)
      .build())
  }

  //关闭客户端
  //最新的close方法可能存在问题，不建议使用
  def close(client: JestClient): Unit = {
    if (Objects.isNull(client)) try {
      client.shutdownClient()
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  //批量插入的方法
  def insertEsByBulk(indexName: String, list: List[(String, Any)]): Unit = {

    if (list.length > 0) {
      val jest: JestClient = getClient

      //构建
      val bulkBuilder = new Bulk.Builder().defaultIndex(indexName).defaultType("_doc")

      //遍历list,封装成Index
      for ((key, value) <- list) {

        //给每一条数据构建Index
        val index: Index = new Index.Builder(value).id(key).build()

        //将Index添加到批处理中
        bulkBuilder.addAction(index)
      }

      //批量插入数据
      val items: util.List[BulkResult#BulkResultItem] = jest.execute(bulkBuilder.build()).getItems
      println(s"成功插入${items.size()}条数据")

      //关闭连接
      jest.close()
      close(jest)
    }
  }

  def main(args: Array[String]): Unit = {

    //获取客户端
    val jest: JestClient = getClient

    //存入ES
    val index: Index = new Index.Builder(Stu("jack", 19))
      .index("test")
      .`type`("_doc")
      .id("2")
      .build()

    //执行
    jest.execute(index)

    //关闭连接
    close(jest)
  }

  case class Stu(name: String, age: Long)

}
