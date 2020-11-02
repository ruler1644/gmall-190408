package com.java.gmallpublish;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


@SpringBootApplication
@MapperScan(basePackages = "com.java.gmallpublish.mapper")
public class GmallPublishDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(GmallPublishDemoApplication.class, args);
    }

}
