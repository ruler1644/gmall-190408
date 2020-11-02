package com.atguigu.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.constants.GmallConstants
import com.atguigu.handler.DAUHandler
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

//TODO　需求一： DAU日活
object StartUpApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("gfd")
    val ssc = new StreamingContext(conf, Seconds(5))

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")

    //读取kafka启动日志主题的数据
    val kafkaStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc, Set(GmallConstants.GMALL_STARTUP))

    //测试
    //kafkaStream.map(_._2).print()

    //转换为样例类对象
    val startUpLogDStream: DStream[StartUpLog] = kafkaStream.map { case (key, value) =>

      val startUpLog: StartUpLog = JSON.parseObject(value, classOf[StartUpLog])

      //取出时间戳
      val ts: Long = startUpLog.ts
      val dateHour: String = sdf.format(new Date(ts))
      val dateHourArr: Array[String] = dateHour.split(" ")

      //给startUpLog对象属性赋值
      startUpLog.logDate = dateHourArr(0)
      startUpLog.logHour = dateHourArr(1)

      //返回
      startUpLog
    }

    //Redis去除重复值(不同批次间)
    //如：redis中已经存在数据[1，3，6]。批次数据为[1，2，2，3，4，5，6]
    //redis去重后，数据为[2，2，4，5]-->[2，4，5]
    val filterStartUpLogDStream: DStream[StartUpLog] = DAUHandler.filterDataByRedis(ssc, startUpLogDStream)

    //Spark同一批次之间去除。[2，2，4，5]-->[2，4，5]
    val distinctStartUpLogDStream: DStream[StartUpLog] = filterStartUpLogDStream
      .map(log => (log.mid, log))
      .groupByKey()
      .flatMap { case (mid, logIter) =>
        logIter.toList.take(1)
      }

    distinctStartUpLogDStream.cache()
    distinctStartUpLogDStream.foreachRDD(rdd => {
      println(s"第二次Spark去重后：${rdd.count()}")
      println("********************")
    })

    //TODO 将去重后的数据写入Redis
    DAUHandler.saveUserToRedis(distinctStartUpLogDStream)

    //TODO 将去重后的数据写HBase
    distinctStartUpLogDStream.foreachRDD(rdd => {
      import org.apache.phoenix.spark._

      rdd.saveToPhoenix("GMALL190408_DAU", Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"), new Configuration(), Some("hadoop102,hadoop103,hadoop104:2181"))
    })

    //启动
    ssc.start()
    ssc.awaitTermination()
  }
}
