package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyESUtils, MyKafkaUtil, RedisUtil}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.native.Serialization
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer
;

//TODO　需求四： 多流join
object SaleDetailApp {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("dfg")
    val ssc = new StreamingContext(conf, Seconds(5))


    //SparkStreaming消费kafka三个主题中的数据
    //订单主题
    val orderInfoDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc, Set(GmallConstants.GMALL_ORDER_INFO))

    //订单详情主题
    val orderDetailDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc, Set(GmallConstants.GMALL_ORDER_DETAIL))

    //用户信息主题
    val userInfoDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc, Set(GmallConstants.GMALL_USER_INFO))

    //将订单信息转换为样例类对象(id,orderInfo)
    val idToOrderInfoDStream: DStream[(String, OrderInfo)] = orderInfoDStream.map {
      case (key, value) => {
        val orderInfo: OrderInfo = JSON.parseObject(value, classOf[OrderInfo])

        //手机号脱敏
        val telArr: (String, String) = orderInfo.consignee_tel.splitAt(4)
        orderInfo.consignee_tel = s"${telArr._1}*******"

        //添加日期和时间
        val create_time_arr: Array[String] = orderInfo.create_time.split(" ")
        orderInfo.create_date = create_time_arr(0)
        val create_hour_arr: Array[String] = create_time_arr(1).split(":")
        orderInfo.create_hour = create_hour_arr(0)

        //返回封装好的样例类对象---->(订单id，订单详情)
        (orderInfo.id, orderInfo)
      }
    }

    //将订单详情转换为样例类对象(order_id,orderDetail)
    val orderIdToOrderDetailDStream: DStream[(String, OrderDetail)] = orderDetailDStream.map {
      case (key, value) => {
        val orderDetail: OrderDetail = JSON.parseObject(value, classOf[OrderDetail])
        (orderDetail.order_id, orderDetail)
      }
    }

    //TODO 订单表join订单详情表
    val orderJoinDetailDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = idToOrderInfoDStream.fullOuterJoin(orderIdToOrderDetailDStream)

    val saleDetailDStream: DStream[SaleDetail] = orderJoinDetailDStream.mapPartitions(iter => {

      //获取Redis连接
      val jedis: Jedis = RedisUtil.getJedisClient
      val saleDetailList = new ListBuffer[SaleDetail]

      //对每条数据进行操作
      iter.foreach { case (orderId, (orderInfoOption, orderDetailOption)) => {

        //TODO 主表orderInfoOption不为空。分为3种情况
        if (orderInfoOption.isDefined) {
          val orderInfo: OrderInfo = orderInfoOption.get

          //TODO  1:orderDetailOption不为空
          if (orderDetailOption.isDefined) {
            val orderDetail: OrderDetail = orderDetailOption.get

            //将关联上的数据添加至集合，传出
            val saleDetail = new SaleDetail(orderInfo, orderDetail)
            saleDetailList += saleDetail
          }

          //TODO 2:orderInfo表不为空，orderDetail没有关联数据，将orderInfo信息写入缓存Redis
          //(key:String,value:String) ----> (order:Info:$orderId,orderInfoJson)
          val orderInfoKey = s"order_info:$orderId"

          //FastJson可以将str转为case class，但是不能将scala中的case class转为str
          //val orderInfo: OrderInfo = JSON.parseObject("xxx", classOf[OrderInfo])  √
          //val str: String = JSON.toJSONString(orderInfo)                          ×

          //Json4s依赖，实现scala对象转为str
          import org.json4s.DefaultFormats
          implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
          val orderInfoJson: String = Serialization.write(orderInfo)

          //key有效期为300秒
          jedis.setex(orderInfoKey, 300, orderInfoJson)

          //TODO 3:查询Redis中orderDetail的数据。一个订单对应多个订单详情，订单详情使用set存储
          //key如果是order:$orderId的话，因为一个订单包含多个订单详情，每一个订单详情信息都会覆盖上一次的记录
          val orderDetailKey = s"order_detail:$orderId"
          val orderDetailSet: java.util.Set[String] = jedis.smembers(orderDetailKey)
          if (!orderDetailSet.isEmpty) {

            //Scala集合转成Java集合对象
            import collection.JavaConversions._
            for (orderDetailJson: String <- orderDetailSet) {

              //转换为orderDetail对象
              val orderDetail: OrderDetail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
              val saleDetail = new SaleDetail(orderInfo, orderDetail)
              saleDetailList += saleDetail
            }
          }
        }


        //TODO 主表orderInfo为空，从表orderDetail不为空。分为2种情况
        else if (orderDetailOption.isDefined) {
          val orderDetail: OrderDetail = orderDetailOption.get

          //TODO 1:查询orderInfo的缓存
          val orderInfoKey = s"order_info:$orderId"
          val orderInfoJson: String = jedis.get(orderInfoKey)
          if (!orderInfoJson.isEmpty && (orderInfoJson != null)) {

            //转换为orderInfo对象
            val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
            val saleDetail = new SaleDetail(orderInfo, orderDetail)
            saleDetailList += saleDetail
          }

          //TODO 2:将orderDetail转换为json写入Redis
          import org.json4s.DefaultFormats
          implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
          val orderDetailJson: String = Serialization.write(orderDetail)

          val orderDetailKey = s"order_detail:$orderId"
          jedis.sadd(orderDetailKey, orderDetailJson)
          jedis.expire(orderDetailKey, 300)
        }
      }
      }

      //关闭Redis连接
      jedis.close()
      saleDetailList.toIterator
    })


    //测试
    //saleDetailDStream.print(100)


    //TODO　获取用户信息，并将其写入Redis　(用户流产生时间较早，变化缓慢，不设置过期时间)
    userInfoDStream.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {

        //获取Redis连接
        val jedis: Jedis = RedisUtil.getJedisClient

        //遍历数据写入Redis
        iter.foreach {

          //kafka消息格式(key,value)，key是读取的数据key-value的key，没有设置是null
          case (key, userInfoJson) => {
            val userInfo: UserInfo = JSON.parseObject(userInfoJson, classOf[UserInfo])
            val userInfoKey = s"userInfo:${userInfo.id}"
            jedis.set(userInfoKey, userInfoJson)
          }
        }

        //关闭Redis连接
        jedis.close()
      })
    })

    //TODO 查询Redis中的用户信息，并与订单详情聚合
    val fullDetailDStream: DStream[SaleDetail] = saleDetailDStream.mapPartitions(iter => {

      //获取Redis连接
      val jedis: Jedis = RedisUtil.getJedisClient

      //创建集合，用于存放并返回合并userInfo后的SaleDetail
      val saleDetailList = new ListBuffer[SaleDetail]

      //查询数据
      iter.foreach(saleDetail => {
        val userInfoKey = s"userInfo:${saleDetail.user_id}"
        val userInfoStr: String = jedis.get(userInfoKey)

        //拼接用户信息
        if (userInfoStr != null) {
          val userInfo: UserInfo = JSON.parseObject(userInfoStr, classOf[UserInfo])
          saleDetail.mergeUserInfo(userInfo)
          saleDetailList += saleDetail
        }
      })

      //关闭redis连接
      jedis.close()
      saleDetailList.toIterator
    })

    //测试
    fullDetailDStream.print(50)


    //TODO 将数据写入ElasticSearch
    fullDetailDStream.foreachRDD(rdd => {

      //转换数据结构
      val fullDetailRDD: RDD[(String, SaleDetail)] = rdd.map(saleDetail => {
        (saleDetail.order_id, saleDetail)
      })

      //写入ElasticSearch
      fullDetailRDD.foreachPartition(iter => {
        MyESUtils.insertEsByBulk(GmallConstants.ES_GMALL_SALE_DETAIL, iter.toList)
      })
    })

    //启动
    ssc.start()
    ssc.awaitTermination()
  }
}
