package com.slf4u0.eventscollectorservice;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.Test;
import org.junit.jupiter.api.BeforeAll;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;
import java.util.concurrent.TimeUnit;

@Testcontainers
@SpringBootTest
public class EventsCollectorIntegrationTest {

    static final Network network = Network.newNetwork();

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.0"))
            .withNetwork(network);

    @Container
    static GenericContainer<?> schemaRegistry = new GenericContainer<>("confluentinc/cp-schema-registry:7.5.0")
            .withNetwork(network)
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://" + kafka.getNetworkAliases().get(0) + ":9092")
            .dependsOn(kafka);

    @Autowired
    private KafkaTemplate<String, DeviceEvent> kafkaTemplate;

    @BeforeAll
    static void createTopics() throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
            admin.createTopics(
                    java.util.List.of(
                            new NewTopic("events", 3, (short) 1),
                            new NewTopic("devices", 3, (short) 1)
                    )
            ).all().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldProcessEvent() {
//        DeviceEvent event = DeviceEvent.newBuilder()
//                .setEventId("evt-1")
//                .setDeviceId("dev-1")
//                .setTimestamp(System.currentTimeMillis())
//                .setType("TEMP")
//                .setPayload("{\"value\": 25}")
//                .build();
//
//        kafkaTemplate.send("events", event.getDeviceId(), event);

        // Здесь можно добавить проверку через ClickHouse JDBC или мок
        // Например: Thread.sleep(2000); и SELECT из CH
    }

}
