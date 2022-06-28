package com.four5prings.app

import com.alibaba.fastjson.JSON
import com.four5prings.bean.UserInfo
import com.four5prings.constants.GmallConstants
import com.four5prings.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
 * @ClassName UserInfoApp
 * @Description
 * @Author Four5prings
 * @Date 2022/6/28 14:12
 */
object UserInfoApp {
  def main(args: Array[String]): Unit = {
    //TODO 1.创建配置文件对象
    val conf: SparkConf = new SparkConf().setAppName("UserInfoApp").setMaster("local[*]")

    //TODO 2.初始化 SparkStreamingContext
    val ssc = new StreamingContext(conf, Seconds(3))

    //获取kafkaDStream
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_USER, ssc)

    //获取数据流
    val userInfoDStream: DStream[String] = kafkaDStream.map(_.value())

    //存入es
    userInfoDStream.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        //创建连接
        val jedis = new Jedis("hadoop102", 6379)
        iter.foreach(info => {
          val userInfo: UserInfo = JSON.parseObject(info, classOf[UserInfo])
          val userInfoRedisKey: String = "userInfo:" + userInfo.id
          //写入redis缓存
          jedis.set(userInfoRedisKey, info)
        })
        jedis.close()
      })
    })

    //TODO 3.启动SparkStreamingContext 线程
    ssc.start()
    //TODO 4.因为是流式传输不能关闭 将主线程阻塞
    ssc.awaitTermination()
  }
}
