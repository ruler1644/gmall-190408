package com.java.gmallpublish.mapper;

import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

@Repository
public interface OrderMapper {

    double selectOrderAmountTotal(String date);

    List<Map> selectOrderAmountHourMap(String date);
}
