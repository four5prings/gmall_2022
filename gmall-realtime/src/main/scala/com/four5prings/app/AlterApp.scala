package com.four5prings.app

import com.alibaba.fastjson.JSON
import com.four5prings.bean.{CouponAlertInfo, EventLog}
import com.four5prings.constants.GmallConstants
import com.four5prings.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.control.Breaks._
import java.text.SimpleDateFormat
import java.util
import java.util.Date

/**
 * @ClassName AlterApp
 * @Description
 * @Author Four5prings
 * @Date 2022/6/27 23:41
 */
object AlterApp {
  def main(args: Array[String]): Unit = {
    //TODO 1.创建配置文件对象
    val conf: SparkConf = new SparkConf().setAppName("AlterApp").setMaster("local[*]")

    //TODO 2.初始化 SparkStreamingContext
    val ssc = new StreamingContext(conf, Seconds(3))

    //获取kafkaDStream
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT, ssc)
    /*
    同一设备，5分钟内三次及以上用不同账号登录并领取优惠劵，并且过程中没有浏览商品
    5min 开窗
    转换为样例类，同一个mid上 不同的uid有 三个以上且点击优惠券 不浏览商品
    mid 去重uid  使用groupbyKey ，将mid作为k，将数据转换为二元组（mid，数据本身）
     */
    //将kafka中的json数据转化为样例类
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")

    val midToLogDStream: DStream[(String, EventLog)] = kafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val eventLog: EventLog = JSON.parseObject(record.value(), classOf[EventLog])
        eventLog.logDate = sdf.format(new Date(eventLog.ts)).split(" ")(0)
        eventLog.logHour = sdf.format(eventLog.ts).split(" ")(1)
        (eventLog.mid, eventLog)
      })
    })
    //开窗
    val midToLogWindowDStream: DStream[(String, EventLog)] = midToLogDStream.window(Seconds(30))

    //对数据进行groupBykey
    val midToLogIterDStream: DStream[(String, Iterable[EventLog])] = midToLogWindowDStream.groupByKey()

    //对数据进行过滤处理，筛选数据，首先用户得领优惠券，并且用户没有浏览商品行为（将符合这些行为的uid保存下来至set集合）
    val boolDStream: DStream[(Boolean, CouponAlertInfo)] = midToLogIterDStream.mapPartitions(partition => {
      partition.map { case (mid, iter) =>
        //创建集合用于存储
        val uids = new util.HashSet[String]()
        val itemIds = new util.HashSet[String]()
        val events = new util.ArrayList[String]()
        var bool = true
        breakable(
          iter.foreach(log => {
            //所有的行为数据都会存入
            events.add(log.evid)
            if ("clickItem".equals(log.evid)) {
              bool = false
              break
            } else if ("coupon".equals(log.evid)) {
              uids.add(log.uid)
              itemIds.add(log.itemid)
            }
          }),
        )
        (uids.size() >= 3 && bool, CouponAlertInfo(mid, uids, itemIds, events, System.currentTimeMillis()))
      }
    })

    //生成预警日志(将数据保存至CouponAlertInfo样例类中，文档中有)，
    // 条件：符合第七步要求，并且uid个数>=3（主要为“过滤”出这些数据），实质：补全
    val alertDStream: DStream[CouponAlertInfo] = boolDStream.filter(_._1).map(_._2)
    alertDStream.print()

    //9.将预警数据写入ES
    alertDStream.foreachRDD(rdd =>{
      rdd.foreachPartition(iter =>{
        //获取 indexName
        val indexName: String = GmallConstants.ES_ALERT_INDEXNAME

        //将数据转化 使用es 的doc_id去重
        val list: List[(String, CouponAlertInfo)] = iter.toList.map(alter => {
          (alter.mid + alter.ts / 1000 / 60, alter)
        })
        MyEsUtil.insertBulk(indexName,list)
      })
    })

    //TODO 3.启动SparkStreamingContext 线程
    ssc.start()
    //TODO 4.因为是流式传输不能关闭 将主线程阻塞
    ssc.awaitTermination()
  }
}
