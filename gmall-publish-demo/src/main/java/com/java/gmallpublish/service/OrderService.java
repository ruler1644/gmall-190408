package com.java.gmallpublish.service;

import java.util.Map;

public interface OrderService {

    double getOrderAmountTotal(String date);

    Map getOrderAmountHourMap(String date);
}
