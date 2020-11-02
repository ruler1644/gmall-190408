package com.java.gmallpublish.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.java.gmallpublish.service.DauService;
import com.java.gmallpublish.service.OrderService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class DAUController {

    @Autowired
    private DauService dauService;
    @Autowired
    private OrderService orderService;

    @GetMapping("realtime-total")
    public String getTotal(@RequestParam("date") String date) {

        //获取日活数据
        long totalDau = dauService.getTotalDau(date);

        List<Map> res = new ArrayList<Map>();

        HashMap<String, Object> dauMap = new HashMap<String, Object>();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", totalDau);
        res.add(dauMap);

        //获取新增设备数据
        HashMap<String, Object> newMidMap = new HashMap<String, Object>();
        newMidMap.put("id", "new_mid");
        newMidMap.put("name", "新增设备");
        newMidMap.put("value", 233);
        res.add(newMidMap);


        //获取交易额数据
        double orderAmountTotal = orderService.getOrderAmountTotal(date);

        HashMap<String, Object> totalAmountMap = new HashMap<String, Object>();
        totalAmountMap.put("id", "order_amount");
        totalAmountMap.put("name", "新增交易额");
        totalAmountMap.put("value", orderAmountTotal);
        res.add(totalAmountMap);

        //List(HashMap)---->json
        return JSON.toJSONString(res);
    }


    @GetMapping("realtime-hours")
    public String getHourDau(@RequestParam("id") String id, @RequestParam("date") String today) {

        //id不同走的逻辑不同，是活跃，还是新增
        if ("dau".equals(id)) {
            JSONObject jsonObject = new JSONObject();

            //查询今日日活的分时统计
            Map todayHourDau = dauService.getHourDau(today);
            jsonObject.put("today", todayHourDau);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

            //获取昨天日期
            try {
                Date yesterdayDate = DateUtils.addDays(sdf.parse(today), -1);
                String yesterdayStr = sdf.format(yesterdayDate);

                //昨天的分小时活跃
                Map yesterdayHourDau = dauService.getHourDau(yesterdayStr);
                jsonObject.put("yesterday", yesterdayHourDau);

            } catch (Exception e) {
                e.printStackTrace();
            }
            return jsonObject.toJSONString();

        } else if ("order_amount".equals(id)) {

            JSONObject jsonObject = new JSONObject();

            //查询今日交易额的分时统计
            Map todayHourAmount = orderService.getOrderAmountHourMap(today);
            jsonObject.put("today", todayHourAmount);

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

            try {
                //获取昨天日期
                Date yesterdayDate = DateUtils.addDays(sdf.parse(today), -1);
                String yesterdayStr = sdf.format(yesterdayDate);

                //昨天的交易额
                Map yesterdayAmountMap = orderService.getOrderAmountHourMap(yesterdayStr);
                jsonObject.put("yesterday", yesterdayAmountMap);

            } catch (Exception e) {
                e.printStackTrace();
            }
            return jsonObject.toJSONString();
        }
        return "";
    }
}
