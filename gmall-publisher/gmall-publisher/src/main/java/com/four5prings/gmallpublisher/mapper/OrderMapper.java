package com.four5prings.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

public interface OrderMapper {
    public Double selectOrderAmountTotal(String date);

    /*
    +--------------+-------------+
    | CREATE_HOUR  | SUM_AMOUNT  |
    +--------------+-------------+
    +--------------+-------------+
     */
    public List<Map> selectOrderAmountHourMap(String date);
}
