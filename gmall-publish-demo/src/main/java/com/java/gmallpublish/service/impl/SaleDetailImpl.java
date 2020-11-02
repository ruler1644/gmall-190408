package com.java.gmallpublish.service.impl;

import com.atguigu.constants.GmallConstants;
import com.java.gmallpublish.service.SaleDetailService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
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

@Service
public class SaleDetailImpl implements SaleDetailService {


    @Autowired
    private JestClient jestClient;

    @Override
    public Map getSaleDetail(String data, String keyWord, int pageNum, int pageSize) {

        //构建DSL查询语句
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //添加filter条件(时间)
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("dt", data));

        //添加match条件(商品名称)
        boolQueryBuilder.must(new MatchQueryBuilder("sku_name", keyWord).operator(MatchQueryBuilder.Operator.AND));
        searchSourceBuilder.query(boolQueryBuilder);

        //性别聚合组
        TermsBuilder genderAggs = AggregationBuilders.terms("groupby_gender").field("user_gender").size(2);

        //年龄聚合组
        TermsBuilder ageAggs = AggregationBuilders.terms("groupby_age").field("user_age").size(120);
        searchSourceBuilder.aggregation(genderAggs);
        searchSourceBuilder.aggregation(ageAggs);

        //分页
        searchSourceBuilder.from((pageNum - 1) * pageSize);
        searchSourceBuilder.size(pageSize);

        //封装结果数据
        HashMap resultMap = new HashMap();
        try {
            SearchResult searchResult = jestClient.execute(new Search
                    .Builder(searchSourceBuilder.toString())
                    .addIndex(GmallConstants.ES_GMALL_SALE_DETAIL)
                    .addType("_doc")
                    .build());

            //获取总数
            Long total = searchResult.getTotal();

            //获取明细
            List<Map> detailList = new ArrayList<>();
            List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
            for (SearchResult.Hit<Map, Void> hit : hits) {
                Map source = hit.source;
                detailList.add(source);
            }

            //获取性别聚合组数据
            Map<String, Long> genderMap = new HashMap<>();
            List<TermsAggregation.Entry> groupby_gender = searchResult
                    .getAggregations()
                    .getTermsAggregation("groupby_gender")
                    .getBuckets();
            for (TermsAggregation.Entry entry : groupby_gender) {
                genderMap.put(entry.getKey(), entry.getCount());
            }

            //获取年龄聚合组数据
            Map<String, Long> ageMap = new HashMap<>();
            List<TermsAggregation.Entry> groupby_age = searchResult
                    .getAggregations()
                    .getTermsAggregation("groupby_age")
                    .getBuckets();
            for (TermsAggregation.Entry entry : groupby_age) {
                ageMap.put(entry.getKey(), entry.getCount());
            }

            //将数据放入Map
            resultMap.put("total", total);
            resultMap.put("genderMap", genderMap);
            resultMap.put("ageMap", ageMap);
            resultMap.put("detail", detailList);

        } catch (IOException e) {
            e.printStackTrace();
        }
        return resultMap;
    }
}
