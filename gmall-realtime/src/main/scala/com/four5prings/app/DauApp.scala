package com.four5prings.app

import com.alibaba.fastjson.JSON
import com.four5prings.bean.StartUpLog
import com.four5prings.constants.GmallConstants
import com.four5prings.handler.DauHandler
import com.four5prings.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

import java.text.SimpleDateFormat
import java.util.Date

/**
 * @ClassName DauApp
 * @Description
 * @Author Four5prings
 * @Date 2022/6/21 16:28
 */
object DauApp {
  def main(args: Array[String]): Unit = {
    // TODO 1. 创建sparkConf
    val conf: SparkConf = new SparkConf().setAppName("DauAPP").setMaster("local[*]")

    // TODO 2. 创建StreamingContext上下文对象
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    // TODO 3. 连接kafka
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)

    // TODO 4，将json格式数据转换样例类
    // 创建时间解析对象，这里是在dirver端执行的，已实现序列化
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")

    val StartUplogDStream: DStream[StartUpLog] = kafkaDStream.mapPartitions(partition => { //以下代码在executor端执行，driver的代码与之交互需要实现序列化
      partition.map(record => {
        val StartUpLog: StartUpLog = JSON.parseObject(record.value(), classOf[StartUpLog])
        //补充 json样例类中的两个字段logDate&logHour
        // 使用SimpleDateFormat对象编译样例类中的时间戳ts
        val str: String = sdf.format(new Date(StartUpLog.ts))

        // 给自定义字段 赋值
        StartUpLog.logDate = str.split(" ")(0)
        StartUpLog.logHour = str.split(" ")(1)

        StartUpLog
      })
    })
    //测试
    /*StartUplogDStream.cache()
    StartUplogDStream.count().print()*/
    // TODO 5. 批次间去重
    val filterByRedisDStream: DStream[StartUpLog] = DauHandler.filterByRedis(StartUplogDStream)
    /*filterByRedisDStream.cache()
    filterByRedisDStream.count().print()*/
    // TODO 6. 批次内去重
    val filterByGroupDStream: DStream[StartUpLog] = DauHandler.filterByGroup(filterByRedisDStream)
    /*filterByGroupDStream.cache()
    filterByGroupDStream.count().print()*/

    // TODO 7.将去重后的数据保存至Redis，为了下一批数据去重用
    DauHandler.saveToRedis(filterByGroupDStream)

    // TODO 8.将数据保存至Hbase
    filterByGroupDStream.foreachRDD(rdd =>{
      rdd.saveToPhoenix(
        "GMALL2022_DAU",
        Seq("MID","UID","APPID","AREA","OS","CH","TYPE","VS","LOGDATE","LOGHOUR","TS"),
        HBaseConfiguration.create,
        Some("hadoop102,hadoop103,hadoop104:2181")
      )
    })

    // 启动并阻塞
    ssc.start()
    ssc.awaitTermination()

  }
}
