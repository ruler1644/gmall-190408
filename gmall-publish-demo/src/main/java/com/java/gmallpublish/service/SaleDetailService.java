package com.java.gmallpublish.service;

import java.util.Map;

public interface SaleDetailService {

    Map getSaleDetail(String data, String keyWord, int pageNum, int pageSize);
}
