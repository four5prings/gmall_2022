package com.four5prings.gmallpublisher.service.impl;

import com.four5prings.gmallpublisher.mapper.DauMapper;
import com.four5prings.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ClassName PublisherServiceImpl
 * @Description
 * @Author Four5prings
 * @Date 2022/6/22 14:59
 * @Version 1.0
 */
@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    private DauMapper dauMapper;

    @Override
    public Integer getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map getDauTotalHours(String date) {
        //通过调用mapper实现类调用方法，拿到phoenix数据
        List<Map> list = dauMapper.selectDauTotalHourMap(date);

        //创建一个map用来存放数据
        HashMap<String, Long> result  = new HashMap<>();

        for (Map map : list) {
            result.put((String) map.get("LH"),(Long) map.get("CT"));
        }

        return result;
    }
}
