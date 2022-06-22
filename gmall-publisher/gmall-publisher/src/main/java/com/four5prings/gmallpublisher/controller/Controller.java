package com.four5prings.gmallpublisher.controller;

//import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONObject;
import com.four5prings.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName Controller
 * @Description
 * @Author Four5prings
 * @Date 2022/6/22 15:01
 * @Version 1.0
 */
@RestController
public class Controller {

    @Autowired//使用自动注入注解找到实现类
    private PublisherService publisherService;

    @RequestMapping("realtime-total")
    public String realtimeTotal(@RequestParam String date){
        //调用server层实现类方法，该方法逐级调用-mapper层方法，由xml文件识别使用sql语句访问phoenix，获取日活次数
        Integer dauTotal = publisherService.getDauTotal(date);

        //查看数据结构，使用list存放数据，内部数据使用map
        ArrayList<Map> result = new ArrayList<>();

        //创建map
        HashMap<String, Object> dauMap = new HashMap<>();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        dauMap.put("value",dauTotal);

        HashMap<String, Object> devMap = new HashMap<>();
        devMap.put("id","new_mid");
        devMap.put("name","新增设备");
        devMap.put("value",233);

        //存放数据
        result.add(dauMap);
        result.add(devMap);

        return JSONObject.toJSONString(result);
//        return "1";
    }

    @RequestMapping("realtime-hours")
    public String realtimeHours(
            @RequestParam String id,
            @RequestParam String date){
        //通过调用service层的实现类方法，获取当天的数据
        Map todayHourMap  = publisherService.getDauTotalHours(date);

        //因为我们还需要前一天的数据，所以使用日期类获取前一天的日期，然后再次调用service层的方法获取数据
        String yesterday = LocalDate.parse(date).plusDays(-1).toString();

        Map yesterdayHourMap = publisherService.getDauTotalHours(yesterday);

        //创建map集合用于存放结果数据
        HashMap<String, Object> result = new HashMap<>();

        result.put("yesterday",yesterdayHourMap);
        result.put("today",todayHourMap);

        //返回结果
        return JSONObject.toJSONString(result);
    }

}
