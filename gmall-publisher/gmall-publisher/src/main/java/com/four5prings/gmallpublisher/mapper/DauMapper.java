package com.four5prings.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

public interface DauMapper {
    public Integer selectDauTotal(String date);

    /**
     * mapper层这个接口通过spring入口类的注解，定位并调用了resources中的xml文件中的sql语句访问phoenix，拿取数据
     * 这里面的xml文件声明定位到这个类 sql语句中的方法名是固定的，参数返回值类型也要满足配置
     * 此处的返回值是 LOGHOUR lh, count(*) ct 即这里是一个
     */
    public List<Map> selectDauTotalHourMap(String date);
}
