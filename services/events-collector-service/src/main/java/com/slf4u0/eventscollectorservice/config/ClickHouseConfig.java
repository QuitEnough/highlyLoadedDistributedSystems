package com.slf4u0.eventscollectorservice.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "clickhouse")
public class ClickHouseConfig {

    private String url;
    private String username;
    private String password;

}
