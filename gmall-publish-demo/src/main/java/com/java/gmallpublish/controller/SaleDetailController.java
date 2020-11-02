package com.java.gmallpublish.controller;

import com.alibaba.fastjson.JSON;
import com.java.gmallpublish.bean.Option;
import com.java.gmallpublish.bean.Stat;
import com.java.gmallpublish.service.SaleDetailService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
public class SaleDetailController {

    @Autowired
    private SaleDetailService saleDetailService;

    @GetMapping("sale_detail")
    public String getSaleDetail(@RequestParam("date") String date, @RequestParam("keyword") String keyWord, @RequestParam("startpage") int pageNum, @RequestParam("size") int pageSize) {

        Map resultMap = new HashMap();
        Map saleDetail = saleDetailService.getSaleDetail(date, keyWord, pageNum, pageSize);

        //取出数据
        Long total = (Long) saleDetail.get("total");
        Map ageMap = (Map) saleDetail.get("ageMap");
        Map genderMap = (Map) saleDetail.get("genderMap");
        List detailList = (List) saleDetail.get("detail");

        //容器
        ArrayList<Stat> stats = new ArrayList<>();

        //提取性别数据
        Long femaleCount = (Long) genderMap.get("F");
        Long maleCount = (Long) genderMap.get("M");
        double femaleRatio = Math.round(femaleCount * 1000D / total) / 10D;
        double maleRatio = Math.round(maleCount * 1000D / total) / 10D;

        ArrayList<Option> genderOptions = new ArrayList<>();
        genderOptions.add(new Option("男", maleRatio));
        genderOptions.add(new Option("女", femaleRatio));

        //存放性别数据
        stats.add(new Stat("用户性别占比", genderOptions));

        //提取年龄数据
        Long age_20Count = 0L;
        Long age_20_30Count = 0L;
        Long age_30_Count = 0L;
        for (Object o : ageMap.entrySet()) {
            Map.Entry entry = (Map.Entry) o;
            int age = Integer.parseInt((String) entry.getKey());
            Long ageCount = (Long) entry.getValue();

            //判断年龄范围
            if (age < 20) {
                age_20_30Count += ageCount;
            } else if (age <= 30) {
                age_20_30Count += ageCount;
            } else {
                age_30_Count += ageCount;
            }
        }

        //各个年龄段的比例
        double age_20Ratio = Math.round(age_20Count * 1000D / total) / 10D;
        double age_20_30Ratio = Math.round(age_20_30Count * 1000D / total) / 10D;
        double age_30_Ratio = Math.round(age_30_Count * 1000D / total) / 10D;

        //存放年龄数据
        ArrayList<Option> ageOptions = new ArrayList<>();
        ageOptions.add(new Option("20岁以下", age_20Ratio));
        ageOptions.add(new Option("20岁到30岁", age_20_30Ratio));
        ageOptions.add(new Option("30岁以上", age_30_Ratio));

        stats.add(new Stat("用户年龄占比", ageOptions));

        //处理成json
        resultMap.put("total", total);
        resultMap.put("detail", detailList);
        resultMap.put("stat", stats);

        return JSON.toJSONString(resultMap);
    }
}
