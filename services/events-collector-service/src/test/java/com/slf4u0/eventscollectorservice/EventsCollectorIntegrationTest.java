package com.slf4u0.eventscollectorservice;

import com.clickhouse.jdbc.ClickHouseDataSource;
import com.slf4u0.avro.DeviceEvent;
import com.slf4u0.eventscollectorservice.outbox.OutboxProcessor;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.lettuce.core.RedisClient;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.*;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles("test")
public class EventsCollectorIntegrationTest {

    // === КЛЮЧЕВОЙ БЛОК: принудительно указываем использовать TCP ===
    static {
        System.setProperty("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE", "tcp://localhost:2375");
        // ИЛИ альтернатива:
        // System.setProperty("DOCKER_HOST", "tcp://localhost:2375");
    }

    @Test
    void dockerShouldWork() {
        try (GenericContainer<?> redis = new GenericContainer<>("redis:7.2")) {
            redis.start();
            assertThat(redis.isRunning()).isTrue();
        }
    }

    static final String KAFKA_TOPIC_EVENTS = "events";
    static final String KAFKA_TOPIC_DEVICES = "devices";

    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.7.0")
                    .asCompatibleSubstituteFor("apache/kafka")
    )
            .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true");

    @Container
    static GenericContainer<?> schemaRegistry = new GenericContainer<>("confluentinc/cp-schema-registry:7.7.0")
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://" + kafka.getNetworkAliases().get(0) + ":9092")
            .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
            .withExposedPorts(8081);

    @Container
    static ClickHouseContainer clickhouse = new ClickHouseContainer("clickhouse/clickhouse-server:24.8")
            .withDatabaseName("default");

    @Container
    static GenericContainer<?> redis = new GenericContainer<>("redis:7.2")
            .withExposedPorts(6379);


    private static KafkaProducer<String, Object> producer;
    private static KafkaConsumer<String, String> devicesConsumer;
    private static ClickHouseDataSource clickHouseDataSource;
    private static RedisClient redisClient;

    @Autowired
    private OutboxProcessor outboxProcessor;

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.kafka.consumer.properties.schema.registry.url",
                () -> "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getFirstMappedPort());
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getFirstMappedPort());
        registry.add("clickhouse.url", clickhouse::getJdbcUrl);
    }

    @SneakyThrows
    @BeforeAll
    static void setUp() {
        // Kafka producer (Avro)
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        producerProps.put("schema.registry.url", "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getFirstMappedPort());

        producer = new KafkaProducer<>(producerProps);

        // Consumer for 'devices'
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        devicesConsumer = new KafkaConsumer<>(consumerProps);
        devicesConsumer.subscribe(Collections.singletonList(KAFKA_TOPIC_DEVICES));

        // ClickHouse
        clickHouseDataSource = new ClickHouseDataSource(clickhouse.getJdbcUrl());

        // Create tables
        try (Connection conn = clickHouseDataSource.getConnection()) {
            conn.createStatement().execute("""
                CREATE TABLE IF NOT EXISTS device_events (
                    deviceId String,
                    eventType String,
                    timestamp Int64
                ) ENGINE = MergeTree() ORDER BY timestamp
                """);

            conn.createStatement().execute("""
                CREATE TABLE IF NOT EXISTS device_outbox (
                    deviceId String,
                    status UInt8,
                    attempts UInt32,
                    lastError Nullable(String)
                ) ENGINE = MergeTree() ORDER BY deviceId
                """);
        }

        // Redis
        redisClient = RedisClient.create("redis://" + redis.getHost() + ":" + redis.getFirstMappedPort());
    }

    @AfterAll
    static void tearDown() {
        producer.close();
        devicesConsumer.close();
        redisClient.shutdown();
    }

    @Test
    public void shouldProcessNewDeviceEventAndCreateOutboxRecord() throws Exception {
        // given
        String deviceId = "test-device-" + System.currentTimeMillis();
        DeviceEvent event = DeviceEvent.newBuilder()
                .setDeviceId(deviceId)
                .setType("test")
                .setTimestamp(Instant.now().toEpochMilli())
                .build();

        // when
        producer.send(new ProducerRecord<>(KAFKA_TOPIC_EVENTS, deviceId, event)).get(10, TimeUnit.SECONDS);
        Thread.sleep(3000); // дать время обработчику

        // then: check device_events
        try (Connection conn = clickHouseDataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM device_events WHERE deviceId = ?")) {
            ps.setString(1, deviceId);
            ResultSet rs = ps.executeQuery();
            rs.next();
            assertThat(rs.getLong(1)).isEqualTo(1);
        }

        // then: check device_outbox
        try (Connection conn = clickHouseDataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM device_outbox WHERE deviceId = ? AND status = 0")) {
            ps.setString(1, deviceId);
            ResultSet rs = ps.executeQuery();
            rs.next();
            assertThat(rs.getLong(1)).isEqualTo(1);
        }
    }

    @Test
    public void shouldPublishFromOutboxAndMarkAsSent() throws Exception {
        // given
        String deviceId = "outbox-test-" + System.currentTimeMillis();

        // manually insert into outbox
        try (Connection conn = clickHouseDataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "INSERT INTO device_outbox (deviceId, status, attempts) VALUES (?, 0, 0)")) {
            ps.setString(1, deviceId);
            ps.executeUpdate();
        }

        // when: trigger outbox processing
        outboxProcessor.processOutbox();
        Thread.sleep(2000); // дать время асинхронной отправке

        // then: check Kafka 'devices' topic
        devicesConsumer.poll(java.time.Duration.ofSeconds(5)); // ensure subscription
        devicesConsumer.seekToBeginning(devicesConsumer.assignment());

        var records = devicesConsumer.poll(java.time.Duration.ofSeconds(5));
        assertThat(records.count()).isEqualTo(1);
        ConsumerRecord<String, String> record = records.iterator().next();
        assertThat(record.value()).isEqualTo(deviceId);

        // then: check outbox status = 1
        try (Connection conn = clickHouseDataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT status FROM device_outbox WHERE deviceId = ?")) {
            ps.setString(1, deviceId);
            ResultSet rs = ps.executeQuery();
            rs.next();
            assertThat(rs.getInt(1)).isEqualTo(1);
        }
    }

}
