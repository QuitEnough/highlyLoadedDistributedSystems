package com.slf4u0.devicecollectorservice;

import org.springframework.boot.SpringApplication;

public class TestDeviceCollectorServiceApplication {

    public static void main(String[] args) {
        SpringApplication.from(DeviceCollectorServiceApplication::main).with(TestcontainersConfiguration.class).run(args);
    }

}
