package com.atguigu.handler

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.atguigu.bean.StartUpLog
import com.atguigu.utils.RedisUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.json4s.DateFormat
import redis.clients.jedis.Jedis

object DAUHandler {

  //时间转换对象
  private val sdf = new SimpleDateFormat("yyyy-MM-dd")

  //使用redis实现不同批次间去除重复值
  def filterDataByRedis(ssc: StreamingContext, startUpLogDStream: DStream[StartUpLog]) = {

    startUpLogDStream.transform(rdd => {

      println(s"第一次Redis去重前：${rdd.count()}")

      //获取redis中的历史数据,即mids
      val jedis: Jedis = RedisUtil.getJedisClient

      //TODO redisKey使用当前时间的年月日,一个批次获取一次redis中的历史数据集，但是对于跨天时可能有数据丢失
      //如23：59分的访问记录，00：01分才到达日志服务器，数据会丢失
      //TODO redisKey使用日志中的时间戳，不会丢失数据，但是每一条数据都需要和redis中的历史数据集比较一次，牺牲一些性能
      val date: String = sdf.format(new Date(System.currentTimeMillis()))
      val mids: util.Set[String] = jedis.smembers(s"dau:$date")

      //广播mids变量
      val midsBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(mids)
      jedis.close()

      //去重
      val filterStartUpLogRDD: RDD[StartUpLog] = rdd.filter(log => {
        val midsBCValue: util.Set[String] = midsBC.value
        !midsBCValue.contains(log.mid)
      })

      println(s"第一次Redis去重后：${filterStartUpLogRDD.count()}")

      //返回
      filterStartUpLogRDD
    })
  }


  //将数据存入Redis，为下一批次去重做准备
  def saveUserToRedis(startUpLogDStream: DStream[StartUpLog]) = {

    startUpLogDStream.foreachRDD(log => {
      log.foreachPartition(items => {
        val jedis: Jedis = RedisUtil.getJedisClient
        items.foreach(startUpLog => {

          //set集合存储mid，redisKey---->dau:2019-9-06 [1,2,3,4]
          val redisKey = s"dau:${startUpLog.logDate}"
          jedis.sadd(redisKey, startUpLog.mid)
        })

        //关闭redis连接
        jedis.close()
      })
    })
  }
}
