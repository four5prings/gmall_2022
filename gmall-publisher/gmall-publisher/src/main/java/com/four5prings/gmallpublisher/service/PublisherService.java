package com.four5prings.gmallpublisher.service;

import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Map;


public interface PublisherService {
    // 获取日活总数据
    public Integer getDauTotal(String date);

    public Map getDauTotalHours(String date);

    public Double getOrderAmountTotal(String date);

    public Map<String,Double> getOrderAmountHourMap(String date);

    public Map<String,Object> getSaleDetail(String date,Integer startpage,Integer size,String keyword) throws IOException;
}
