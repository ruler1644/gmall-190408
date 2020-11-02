package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.OrderInfo
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

//TODO　需求二： GMV交易额
object OrderApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("JJd")
    val ssc = new StreamingContext(conf, Seconds(5))

    //消费kafka中order主题上的数据
    val kafkaDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc, Set(GmallConstants.GMALL_ORDER_INFO))

    //格式化数据为orderInfo对象
    val orderInfoDStream: DStream[OrderInfo] = kafkaDStream.map {
      case (key, value) => {

        //转换成orderInfo对象
        val orderInfo: OrderInfo = JSON.parseObject(value, classOf[OrderInfo])

        //对的手机号进行脱敏，从第四个字符开始切分成key-value型的tuple
        val telArr: (String, String) = orderInfo.consignee_tel.splitAt(4)
        orderInfo.consignee_tel = s"${telArr._1}*******"

        //添加日期(2019-02-10 08:52:21)
        val create_time_arr: Array[String] = orderInfo.create_time.split(" ")
        orderInfo.create_date = create_time_arr(0)

        //添加小时
        val create_hour_arr: Array[String] = create_time_arr(1).split(":")
        orderInfo.create_hour = create_hour_arr(0)

        //返回封装好的样例类对象
        orderInfo
      }
    }

    //写入HBase
    orderInfoDStream.foreachRDD(
      rdd => {
        import org.apache.phoenix.spark._
        rdd.saveToPhoenix("GMALL20190408_ORDER_INFO", Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID", "IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME", "OPERATE_TIME", "TRACKING_NO", "PARENT_ORDER_ID", "OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"), new Configuration(), Some("hadoop102,hadoop103,hadoop104:2181"))
      })

    //启动
    ssc.start()
    ssc.awaitTermination()
  }
}
