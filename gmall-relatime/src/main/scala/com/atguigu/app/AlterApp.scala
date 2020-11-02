package com.atguigu.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{AlertInfo, EventInfo, OrderInfo}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyKafkaUtil, MyESUtils}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

//TODO　需求三： 购物券预警
object AlterApp {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("alter")
    val ssc = new StreamingContext(conf, Seconds(5))

    //读取kafka order主题的数据
    val KafkaDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc, Set(GmallConstants.GMALL_EVENT))
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")

    //转换数据格式
    val midToEventDStream: DStream[(String, EventInfo)] = KafkaDStream.map {
      case (key, value) => {

        //样例类对象（key,value）-->（mid,eventInfo）
        val eventInfo: EventInfo = JSON.parseObject(value, classOf[EventInfo])

        //赋值时间
        val dateArr: Array[String] = sdf.format(new Date(eventInfo.ts)).split(" ")
        eventInfo.logDate = dateArr(0)
        eventInfo.logHour = dateArr(1)

        //返回元组
        (eventInfo.mid, eventInfo)
      }
    }

    //开窗并分组，窗口大小是30秒，5秒是一个批次，拿到同一设备访问日志
    val midToEventIterDStream: DStream[(String, Iterable[EventInfo])] = midToEventDStream.window(Seconds(30)).groupByKey()

    //过滤数据的准备工作(mid,[EventInfo...])---->(flag,AlterInfo)
    val flagAlertTODStream: DStream[(Boolean, AlertInfo)] = midToEventIterDStream.map {
      case (mid, eventInfoIter) => {

        //三次不同账号登陆
        val uids = new util.HashSet[String]()

        //领取优惠券（行为），关联的商品Id
        val itemids = new util.HashSet[String]()

        //事件类型：没有浏览过商品（行为）
        val eventTypes = new util.ArrayList[String]()

        //记录是否存在点击行为
        var flag: Boolean = true

        import scala.util.control.Breaks._
        breakable {
          eventInfoIter.foreach(eventInfo => {

            //记录用户的行为
            eventTypes.add(eventInfo.evid)

            //是否为领取优惠券行为日志
            if ("coupon".equals(eventInfo.evid)) {

              //必须是领券行为，同一设备上登陆的账号的个数(若是点赞收藏等行为，则账号不会计入)
              uids.add(eventInfo.uid)

              //领券行为，关联的商品Id
              itemids.add(eventInfo.itemid)

            } else if ("clickItem".equals(eventInfo.evid)) {
              flag = false
              break()
            }
          })
        }

        //返回tuple(boolean,alertInfo)
        (flag && uids.size() >= 3, AlertInfo(mid, uids, itemids, eventTypes, System.currentTimeMillis()))
      }
    }

    //测试，打印
    //flagAlertTODStream.print(100)


    //TODO 过滤数据
    //(true, AlertInfo(mid_83,[226, 361, 355],[27, 41, 6],[addComment, coupon, coupon, addCart, addCart, coupon],1601284510371))
    val alterInfoDStream: DStream[AlertInfo] = flagAlertTODStream.filter(_._1).map(_._2)

    //测试，打印
    //alterInfoDStream.print(10)
    alterInfoDStream.cache()


    //TODO 写入ElasticSearch，使用ElasticSearch做去重(同一设备一分钟只记录一次预警)
    //key的类型是mid_分钟 ----> (mid_分钟,AlertInfo)
    val keyAndValueDStream: DStream[(String, AlertInfo)] = alterInfoDStream.map(alertInfo => {
      val mint: Long = alertInfo.ts / 1000 / 60
      (s"${alertInfo.mid}_$mint", alertInfo)
    })


    keyAndValueDStream.foreachRDD(rdd =>

      //按照分区，批量写入数据
      rdd.foreachPartition(iter => {
        MyESUtils.insertEsByBulk("gmall0408_coupon_alert", iter.toList)
      })
    )

    //测试
    //flagAlertTODStream.print(40)

    //启动
    ssc.start()
    ssc.awaitTermination()
  }
}
