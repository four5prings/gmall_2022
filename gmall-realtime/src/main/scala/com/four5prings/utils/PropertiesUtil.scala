package com.four5prings.utils

/**
 * @ClassName PropertiesUtil
 * @Description
 * @Author Four5prings
 * @Date 2022/6/21 16:25
 */
import java.io.InputStreamReader
import java.util.Properties

object PropertiesUtil {

  def load(propertieName:String): Properties ={
    val prop=new Properties()
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName) , "UTF-8"))
    prop
  }
}

