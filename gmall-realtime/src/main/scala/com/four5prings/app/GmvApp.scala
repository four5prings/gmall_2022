package com.four5prings.app

import com.alibaba.fastjson.JSON
import com.four5prings.bean.OrderInfo
import com.four5prings.constants.GmallConstants
import com.four5prings.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

/**
 * @ClassName GmvApp
 * @Description
 * @Author Four5prings
 * @Date 2022/6/27 22:53
 */
object GmvApp {
  def main(args: Array[String]): Unit = {
    //TODO 1.创建配置文件对象
    val conf: SparkConf = new SparkConf().setAppName("GmvApp").setMaster("local[*]")

    //TODO 2.初始化 SparkStreamingContext
    val ssc = new StreamingContext(conf, Seconds(3))

    // 获取kafkaDStream流
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants
      .KAFKA_TOPIC_ORDER, ssc)

    //将 json数据 转换为样例类
    val orderInfoDStream: DStream[OrderInfo] = kafkaDStream.map(record => {
      val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
      //添加两个字段
      orderInfo.create_date = orderInfo.create_time.split(" ")(0)
      orderInfo.create_hour = orderInfo.create_time.split(" ")(1).split(":")(0)
      orderInfo
    })

    //将数据写入HBase中
    orderInfoDStream.foreachRDD(rdd =>{
      rdd.saveToPhoenix(
        "GMALL2022_ORDER_INFO",
        Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID", "IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME", "OPERATE_TIME", "TRACKING_NO", "PARENT_ORDER_ID", "OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
        HBaseConfiguration.create(),
        zkUrl = Some("hadoop102,hadoop103,hadoop104:2181")
      )
    })


    //TODO 3.启动SparkStreamingContext 线程
    ssc.start()
    //TODO 4.因为是流式传输不能关闭 将主线程阻塞
    ssc.awaitTermination()
  }
}
