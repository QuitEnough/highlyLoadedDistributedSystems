package com.slf4u0.eventscollectorservice.config;

import com.clickhouse.jdbc.ClickHouseDataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

@Configuration
public class ClickHouseConfig {

    @Value("${clickhouse.url}")
    private String url;

    @Value("${clickhouse.username}")
    private String username;

    @Value("${clickhouse.password}")
    private String password;

//    public DataSource clickHouseDataSource() {
//        ClickHouseDataSource ds = new ClickHouseDataSource(url);
//        return ds;
//    }

}
