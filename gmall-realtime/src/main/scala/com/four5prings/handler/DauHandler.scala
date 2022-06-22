package com.four5prings.handler

import com.four5prings.bean.StartUpLog
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

import java.{lang, util}
import java.text.SimpleDateFormat
import java.util.Date


/**
 * @ClassName DauHandler
 * @Description
 * @Author Four5prings
 * @Date 2022/6/21 19:14
 */
object DauHandler {
  /**
   * 批次内 去重
   * @param filterByRedisDStream
   */
  def filterByGroup(filterByRedisDStream: DStream[StartUpLog]) = {
    val value: DStream[StartUpLog] = {
      //1.将数据转化为k，v ((mid,logDate)log)
      val midAndDateToLogDStream: DStream[((String, String), StartUpLog)]
      = filterByRedisDStream.mapPartitions(partition => {
        partition.map(log => {
          ((log.mid, log.logDate), log)
        })
      })

      //2.groupByKey将相同key的数据聚和到同一个分区中
      val midAndDateToLogIterDStream: DStream[((String, String), Iterable[StartUpLog])]
      = midAndDateToLogDStream.groupByKey()

      //3.将数据排序并取第一条数据
      val midAndDateToLogListDStream: DStream[((String, String), List[StartUpLog])] = midAndDateToLogIterDStream.mapValues(iter => {
        iter.toList.sortWith(_.ts < _.ts).take(1)
      })

      //4.将数据扁平化
      midAndDateToLogListDStream.flatMap(_._2)
    }
    value
  }

  /**
   * 进行批次间去重 将DStream中的每一个rdd中的value，与redis的进行过滤使用fliter
   * @param StartUplogDStream
   */
  def filterByRedis(StartUplogDStream: DStream[StartUpLog]) = {
    /**
     * 方案一 ： 这里是每一个rdd使用一次filter，占用资源，可以使用分区算子，一个分区执行一次
     */
//    val value: DStream[StartUpLog] = StartUplogDStream.filter(log => {
//      //创建redis连接
//      val jedis: Jedis = new Jedis("hadoop102", 6379)
//
//      //获取redisKey
//      val redisKey: String = "Dau:" + log.logDate
//
//      val boolean: lang.Boolean = jedis.sismember(redisKey, log.mid)
//
//
//      //关闭连接
//      jedis.close()
//      !boolean
//    })
//    value
    /**
     * 方案二 在分区下创建连接（减少连接个数）
     */
//    val value: DStream[StartUpLog] = StartUplogDStream.mapPartitions(partition => {
//      //创建redis连接
//      val jedis: Jedis = new Jedis("hadoop102", 6379)
//
//      val logs: Iterator[StartUpLog] = partition.filter(log => {
//        //创建redisKey
//        val redisKey: String = "Dau:" + log.logDate
//        //使用redis连接判断 redis中是否存在该key
//        !jedis.sismember(redisKey, log.mid)
//      })
//      //关闭jedis连接
//      jedis.close()
//      logs
//    })
//    value

    /**
     * 方案三 在每个批次下创建连接
     * 技术难点： 1. 每个批次创建连接，那么就需要在driver端创建连接，而rdd需要在executor端执行，连接不能序列化，
     *              所以使用广播变量，将redis的所有value广播到executor端
     *          2. driver端拿不到rdd中的log.logdate，这里直接使用当前时间
     */
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val value: DStream[StartUpLog] = StartUplogDStream.transform(rdd => {
      //创建redis连接
      val jedis: Jedis = new Jedis("hadoop102", 6379)

      //使用当前时间创建redisKey
      val RedisKey: String = "Dau:" + sdf.format(new Date(System.currentTimeMillis()))

      //拿到所有jedis该key的value
      val mids: util.Set[String] = jedis.smembers(RedisKey)

      //将所有的mids广播到
      val broadcastMids: Broadcast[util.Set[String]] = rdd.context.broadcast(mids)

      val midFilterRDD: RDD[StartUpLog] = rdd.filter(log => {
        !broadcastMids.value.contains(log)
      })
      //关闭jedis连接
      jedis.close()
      midFilterRDD
    })
    value
  }

  /**
   * 将去重后的数据保存至Redis，为了下一批数据去重用
   *
   * @param StartUplogDStream
   */
  def saveToRedis(StartUplogDStream: DStream[StartUpLog]) ={
    //将去重的数据存入 redis中
    StartUplogDStream.foreachRDD(rdd =>{
      //如果在此处创建redis连接，因为是在driver端，会因为序列化导致错误。
      rdd.foreachPartition(partition =>{
        //创建redis连接- 在此处创建连接可以减少连接个数，且此处的是在executor端
        val jedis = new Jedis("hadoop102", 6379)
        partition.foreach(log =>{
          val RedisKey: String = "Dau:" + log.logDate
          jedis.sadd(RedisKey,log.mid)
        })
        //关闭redis连接
        jedis.close()
      })
    })
  }

}
