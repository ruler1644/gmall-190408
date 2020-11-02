package com.java.gmallpublish.mapper;

import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

@Repository
public interface DauMapper {

    /**
     * 获取每日新增
     *
     * @param date
     * @return
     */
    long getTotalDau(String date);


    /**
     * 分小时的活跃
     *
     * @param date
     * @return
     */
    List<Map> selectDauTotalHourMap(String date);
}
