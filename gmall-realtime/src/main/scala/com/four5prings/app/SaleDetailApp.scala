package com.four5prings.app

import com.alibaba.fastjson.JSON
import com.four5prings.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.four5prings.constants.GmallConstants
import com.four5prings.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import collection.JavaConverters._
import org.json4s.native.Serialization

import java.text.SimpleDateFormat
import java.util
import java.util.Date

/**
 * @ClassName SaleDetailApp
 * @Description
 * @Author Four5prings
 * @Date 2022/6/28 0:24
 */
object SaleDetailApp {
  def main(args: Array[String]): Unit = {
    //1 创建sparkconf
    val conf: SparkConf = new SparkConf().setAppName("SaleDetailApp").setMaster("local[*]")
    //2 创建streamingcontext
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
    //3 获取kafkaDStream
    val orderInfoDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants
      .KAFKA_TOPIC_ORDER, ssc)
    val orderDetailDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants
      .KAFKA_TOPIC_ORDER_DETAIL, ssc)
    //4 转换json格式为样例类 -这里双流join，所以需要转换为二元组的形式
    val idToInfoDStream: DStream[(String, OrderInfo)] = orderInfoDStream.mapPartitions(partition => {
      partition.map(record => {
        val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
        //添加字段
        orderInfo.create_hour = orderInfo.create_time.split(" ")(0)
        orderInfo.create_hour = orderInfo.create_time.split(" ")(1).split(":")(0)
        (orderInfo.id, orderInfo)
      })
    })
    val idToDetailDStream: DStream[(String, OrderDetail)] = orderDetailDStream.mapPartitions(partition => {
      partition.map(record => {
        val orderDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
        (orderDetail.order_id, orderDetail)
      })
    })

    //5 双流join
    val joinWithoutUserDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = idToInfoDStream
      .fullOuterJoin(idToDetailDStream)

    /**
     * 此时 DStream中的数据，是（orderid，（二元组）），这里会出现三种情况，
     * orderopt不为空，detailopt不为空，这里是join上
     * orderopt为空，detailopt不为空，可能存在网络延迟，将detail缓存起来，并给一个缓存时间
     * orderopt不为空，detailopt为空，可能存在网络延迟，查找detail缓存
     * 未写完
     */
    //6 将orderInfo 与orderdetil 合并为 saledetail将数据存入redis，解决网络延迟问题
    val SaleWithoutUserDStream: DStream[SaleDetail] = joinWithoutUserDStream.mapPartitions(iter => {
      //创建redis连接
      val jedis = new Jedis("hadoop102", 6379)
      //创建一个list存放最终的结果集，就是saledetail，这样返回的结果就是dstream
      val targetlist: util.ArrayList[SaleDetail] = new util.ArrayList[SaleDetail]()
      //遍历迭代器，使用偏函数方式。判断
      iter.foreach { case (orderId, (orderInfoOpt, orderDetailOpt)) =>
        implicit val formats = org.json4s.DefaultFormats
        //如果orderinfo存在
        val orderInfoRedisKey: String = "order:" + orderId
        val orderDetailRedisKey: String = "detail:" + orderId

        if (orderInfoOpt.isDefined) {
          //存在获取orderInfo样例类
          val orderInfo: OrderInfo = orderInfoOpt.get
          //1. 查看orderDeatil是否存在，存在就是可以关联上的，那么执行关联操作（存入saledetail）
          if (orderDetailOpt.isDefined) {
            val orederDetail: OrderDetail = orderDetailOpt.get
            targetlist.add(new SaleDetail(orderInfo, orederDetail))
          }
          //2. 将orderinfo存入redis缓存中 数据结构是String
          //导入隐式参数及依赖，将样例类转化为json字符串
          val orderInfoJson: String = Serialization.write(orderInfo)
          //将json字符串存入
          jedis.set(orderInfoRedisKey, orderInfoJson)
          jedis.expire(orderInfoRedisKey, 30)

          //3. 查看orderdetail缓存是否有对应的orderid，如果有则证明可以关联上，执行saledetail并存入操作
          val orderDetailCacheSet: util.Set[String] = jedis.smembers(orderDetailRedisKey)
          //遍历这个集合，这里将集合转化为scala
          orderDetailCacheSet.asScala.foreach(detail => {
            val orderDetail: OrderDetail = JSON.parseObject(detail, classOf[OrderDetail])
            targetlist.add(new SaleDetail(orderInfo, orderDetail))
          })

        } else { //orderinfo不存在，此时的orderdetail一定存在，因为不可能会有一个fulljoin的值是两个空值
          //1. 查看redis中的orderinfo缓存，如果存在，那么他们的orderid相同，代表可以关联，执行关联操作
          val orderDetail: OrderDetail = orderDetailOpt.get
          if (jedis.exists(orderInfoRedisKey)) {
            /*val orderCacheSet: util.Set[String] = jedis.smembers(orderInfoRedisKey)
            // 遍历redis中含有与detailid相同id的orderid缓存
            orderCacheSet.asScala.foreach(order =>{
              val orderInfo: OrderInfo = JSON.parseObject(order, classOf[OrderInfo])
              targetlist.add(new SaleDetail(orderInfo,orderDetail))
            })*/
            val orderJsonStr: String = jedis.get(orderInfoRedisKey)
            val orderInfo: OrderInfo = JSON.parseObject(orderJsonStr, classOf[OrderInfo])
            targetlist.add(new SaleDetail(orderInfo,orderDetail))
          } else { //2.else 没关联上的，存放入缓存中，数据结构是Set
            val orderDetailJson: String = Serialization.write(orderDetail)
            jedis.sadd(orderDetailRedisKey, orderDetailJson)
            jedis.expire(orderDetailRedisKey, 30)
          }
        }
      }
      //关闭连接
      jedis.close()
      //返回结果集
      targetlist.asScala.toIterator
    })

    //从redis中查询userid信息
    val saleDetailDStream: DStream[SaleDetail] = SaleWithoutUserDStream.mapPartitions(partition => {
      val jedis = new Jedis("hadoop102", 6379)
      val details: Iterator[SaleDetail] = partition.map(saleDetail => {

        //根据redisKey获取数据
        val userRedisKey: String = "userInfo:" + saleDetail.user_id
        val userJsonStr: String = jedis.get(userRedisKey)
        //转化为样例类
        val userInfo: UserInfo = JSON.parseObject(userJsonStr, classOf[UserInfo])
        //调用方法
        saleDetail.mergeUserInfo(userInfo)
        saleDetail
      })
      //关闭连接
      jedis.close()
      details
    })
    saleDetailDStream.print()

    //将数据写入 es，es这里调用了myesutil工具类，需要传入一个docId和list
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    saleDetailDStream.foreachRDD(rdd =>{
      rdd.foreachPartition(iter =>{
        //创建一个索引名
        val time: String = sdf.format(new Date(System.currentTimeMillis()))
        val indexName: String = GmallConstants.ES_DETAIL_INDEXNAME + time
        //转换样例类
        val list: List[(String, SaleDetail)] = iter.toList.map(saleDetail =>{
          (saleDetail.order_detail_id, saleDetail)
        })
        MyEsUtil.insertBulk(indexName,list)
      })
    })
    //启动并阻塞
    ssc.start()
    ssc.awaitTermination()
  }
}
