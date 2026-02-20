package com.slf4u0.devicecollectorservice;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@Import(TestcontainersConfiguration.class)
@SpringBootTest
class DeviceEntityCollectorServiceApplicationTests {

    @Test
    void contextLoads() {
    }

}
