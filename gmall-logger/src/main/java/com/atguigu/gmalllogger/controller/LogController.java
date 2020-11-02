package com.atguigu.gmalllogger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.constants.GmallConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;


//@Controller+@ResponseBody
@RestController
@Slf4j
public class LogController {

    @GetMapping("test")
    public String getTest(@RequestParam("aa") String str) {
        System.out.println("!!!!!!");
        return str;
    }

    //editor-->inspections-->spring core-->core-->autowiring for bean class
    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @PostMapping("log")
    public String doLog(@RequestParam("logString") String str) {

        //System.out.println(str);

        //1.添加时间
        JSONObject jsonObject = JSON.parseObject(str);
        jsonObject.put("ts", System.currentTimeMillis());
        String jsonStr = jsonObject.toString();

        //2.写入本地文件
        log.info(jsonStr);

        //3.写往Kafka(判断具体是哪种日志)
        if ("startup".equals(jsonObject.getString("type"))) {
            kafkaTemplate.send(GmallConstants.GMALL_STARTUP, jsonStr);
        } else {
            kafkaTemplate.send(GmallConstants.GMALL_EVENT, jsonStr);
        }
        return "success";
    }

}