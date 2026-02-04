package com.slf4u0.eventscollectorservice.config;

import com.zaxxer.hikari.HikariDataSource;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

@Configuration
@RequiredArgsConstructor
public class DataSourceConfig {

    private final ClickHouseConfig clickHouseConfig;

    @Bean
    public DataSource clickhouseDataSource() {
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setJdbcUrl(clickHouseConfig.getUrl());
        dataSource.setUsername(clickHouseConfig.getUsername());
        dataSource.setPassword(clickHouseConfig.getPassword());
        dataSource.setMaximumPoolSize(10);
        dataSource.setConnectionTimeout(30_000);
        return dataSource;
    }

}
