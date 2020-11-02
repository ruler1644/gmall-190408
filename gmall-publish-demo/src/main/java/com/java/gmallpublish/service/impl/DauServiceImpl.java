package com.java.gmallpublish.service.impl;

import com.java.gmallpublish.mapper.DauMapper;
import com.java.gmallpublish.service.DauService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Service
public class DauServiceImpl implements DauService {

    @Autowired
    private DauMapper dauMapper;


    @Override
    public long getTotalDau(String date) {
        return dauMapper.getTotalDau(date);
    }


    @Override
    public Map getHourDau(String date) {

        //定义返回值类型
        HashMap<String, Long> hourDauMap = new HashMap<String, Long>();
        List<Map> lists = dauMapper.selectDauTotalHourMap(date);

        //打印日期测试，打印key
        System.out.println(date);
        for (Map map : lists) {
            Set set = map.keySet();
            for (Object o : set) {
                System.out.println(o);
            }
        }

        for (Map map : lists) {
            String lh = (String) map.get("LH");
            long ct = (long) map.get("CT");

            System.out.println(lh + "**************" + ct);
            hourDauMap.put(lh, ct);
        }

        return hourDauMap;
    }
}
