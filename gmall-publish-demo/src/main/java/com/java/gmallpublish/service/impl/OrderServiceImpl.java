package com.java.gmallpublish.service.impl;

import com.java.gmallpublish.mapper.OrderMapper;
import com.java.gmallpublish.service.OrderService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Service
public class OrderServiceImpl implements OrderService {

    @Autowired
    private OrderMapper orderMapper;

    @Override
    public double getOrderAmountTotal(String date) {
        return orderMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map getOrderAmountHourMap(String date) {

        //定义返回值类型
        HashMap<String, Double> hourAmountMap = new HashMap<String, Double>();
        List<Map> lists = orderMapper.selectOrderAmountHourMap(date);

        for (Map map : lists) {
            String lh = (String) map.get("CREATE_HOUR");
            Double ct = (Double) map.get("SUM_AMOUNT");

            System.out.println(lh + "**************" + ct);
            hourAmountMap.put(lh, ct);
        }
        return hourAmountMap;
    }
}
