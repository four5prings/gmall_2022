package com.four5prings.gmallpublisher.service;

import org.springframework.stereotype.Service;

import java.util.Map;


public interface PublisherService {
    // 获取日活总数据
    public Integer getDauTotal(String date);

    public Map getDauTotalHours(String date);
}