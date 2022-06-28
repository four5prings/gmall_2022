package com.four5prings.gmallpublisher.service.impl;

import com.four5prings.constants.GmallConstants;
import com.four5prings.gmallpublisher.bean.Option;
import com.four5prings.gmallpublisher.bean.Stat;
import com.four5prings.gmallpublisher.mapper.DauMapper;
import com.four5prings.gmallpublisher.mapper.OrderMapper;
import com.four5prings.gmallpublisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
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
    @Autowired
    private OrderMapper orderMapper;

    @Autowired
    private JestClient jestClient;

    @Override
    public Integer getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map getDauTotalHours(String date) {
        //通过调用mapper实现类调用方法，拿到phoenix数据
        List<Map> list = dauMapper.selectDauTotalHourMap(date);

        //创建一个map用来存放数据
        HashMap<String, Long> result = new HashMap<>();

        for (Map map : list) {
            result.put((String) map.get("LH"), (Long) map.get("CT"));
        }

        return result;
    }

    @Override
    public Double getOrderAmountTotal(String date) {
        return orderMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map<String, Double> getOrderAmountHourMap(String date) {
        List<Map> list = orderMapper.selectOrderAmountHourMap(date);
        HashMap<String, Double> result = new HashMap<>();
        for (Map map : list) {
            result.put((String) map.get("CREATE_HOUR"), (Double) map.get("SUM_AMOUNT"));
        }
        return result;
    }

    @Override
    public Map<String, Object> getSaleDetail(String date, Integer startpage, Integer size, String keyword) throws IOException {
        //创建searc资源对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //过滤匹配
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("dt", date));
        boolQueryBuilder.must(new MatchQueryBuilder("sku_name", keyword));

        searchSourceBuilder.query(boolQueryBuilder);
        //聚合匹配
        //性别聚合
        TermsBuilder ageAggs = AggregationBuilders.terms("groupby_user_age").field("user_age");
        //年龄聚合
        TermsBuilder genderAggs = AggregationBuilders.terms("groupby_user_gender").field("user_gender");
        searchSourceBuilder.aggregation(ageAggs);
        searchSourceBuilder.aggregation(genderAggs);

        //页面
        searchSourceBuilder.from((startpage - 1) * size);
        searchSourceBuilder.size(size);

        Search search = new Search.Builder(searchSourceBuilder.toString())
                .addIndex(GmallConstants.ES_DETAIL_INDEXNAME)
                .addType("_doc")
                .build();
        SearchResult searchResult = jestClient.execute(search);

        //获取数据
        //TODO 1.获取总数
        Long total = searchResult.getTotal();

        ArrayList<Map> detail = new ArrayList<>();
        //TODO 2.获取明细数据
        List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            detail.add(hit.source);
        }

        //TODO 3.获取聚合数据
        MetricAggregation aggregations = searchResult.getAggregations();
        //创建 options集合用来存储数据
        ArrayList<Option> genderOptions = new ArrayList<>();
        ArrayList<Option> ageOptions = new ArrayList<>();
        //获取年龄的占比
        TermsAggregation groupbyAge = aggregations.getTermsAggregation("groupby_user_age");
        List<TermsAggregation.Entry> buckets1 = groupbyAge.getBuckets();

        Long low20Count = 0L;
        Long up30Count = 0L;
        for (TermsAggregation.Entry bucket : buckets1) {
            if (Integer.parseInt(bucket.getKey()) < 20) {
                low20Count += bucket.getCount();
            } else if (Integer.parseInt(bucket.getKey()) > 30) {
                up30Count += bucket.getCount();
            }
        }
        //获取小于20岁的占比
        double low20Ratio = Math.round(low20Count * 1000d / total) / 10D;
        double up30Ratio = Math.round(up30Count * 1000d / total) / 10D;
        double up20AndLow30Ratio = Math.round(100D - low20Ratio - up30Ratio);

        Option low20Opt = new Option("20岁以下", low20Ratio);
        Option up30Opt = new Option("30岁及30岁以上", up30Ratio);
        Option up20AndLow30Opt = new Option("20岁到30岁", up20AndLow30Ratio);
        ageOptions.add(low20Opt);
        ageOptions.add(up30Opt);
        ageOptions.add(up20AndLow30Opt);
        //创建年龄占比的Stat对象
        Stat ageStat = new Stat("用户年龄占比", ageOptions);

        //获取 性别的占比
        TermsAggregation groupbyGender = aggregations.getTermsAggregation("groupby_user_gender");
        List<TermsAggregation.Entry> buckets = groupbyGender.getBuckets();
        //创建男性个数
        Long male = 0l;
        for (TermsAggregation.Entry bucket : buckets) {
            if ("M".equals(bucket.getKey())) {
                male += bucket.getCount();
            }
        }
        //求出男性占比
        double maleRatio = Math.round(male * 1000d / total) / 10d;
        double femaleRatio = Math.round(100d - maleRatio);

        //创建options对象
        Option maleOpt = new Option("男", maleRatio);
        Option femaleOpt = new Option("女", femaleRatio);
        genderOptions.add(maleOpt);
        genderOptions.add(femaleOpt);
        Stat genderStat = new Stat("用户性别占比", genderOptions);

        //创建list集合存储所有stat对象
        ArrayList<Stat> stats = new ArrayList<>();
        stats.add(genderStat);
        stats.add(ageStat);

        //创建Map集合用来存放结果数据
        HashMap<String, Object> result = new HashMap<>();
        result.put("total", total);
        result.put("stat", stats);
        result.put("detail", detail);

        return result;
    }
}
