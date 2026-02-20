package com.slf4u0.devicecollectorservice.config;

import com.slf4u0.avro.Device;
import com.slf4u0.devicecollectorservice.exception.NotRetryableException;
import com.slf4u0.devicecollectorservice.exception.RetryableException;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    @Autowired
    Environment environment;

    @Bean
    ConsumerFactory<String, Device> consumerFactory() {
        Map<String, Object> config = new HashMap<>();

        config.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                environment.getProperty("spring.kafka.bootstrap-servers")
        );

        config.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class
        );

        config.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ErrorHandlingDeserializer.class
        );

        config.put(
                ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS,
                KafkaAvroDeserializer.class
        );

        config.put(
                "schema.registry.url",
                environment.getProperty("spring.kafka.consumer.properties.schema.registry.url")
        );

        config.put(
                "specific.avro.reader",
                environment.getProperty(
                        "spring.kafka.consumer.properties.specific.avro.reader",
                        Boolean.class,
                        true
                        )
        );

        config.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                environment.getProperty("spring.kafka.consumer.group-id")
        );

        config.put(
                ConsumerConfig.ISOLATION_LEVEL_CONFIG,
                environment.getProperty(
                        "spring.kafka.consumer.isolation-level",
                        "READ_COMMITTED").toLowerCase()
        );

        config.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                environment.getProperty("spring.kafka.consumer.auto-offset-reset")
        );

        return new DefaultKafkaConsumerFactory<>(config);
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, Device> kafkaListenerContainerFactory(
            ConsumerFactory<String, Device> consumerFactory,
            KafkaTemplate<String, Device> kafkaTemplate
    ) {
        ConcurrentKafkaListenerContainerFactory<String, Device> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                new DeadLetterPublishingRecoverer(kafkaTemplate),
                new FixedBackOff(5000, 3)
        );
        errorHandler.addNotRetryableExceptions(NotRetryableException.class);
        errorHandler.addRetryableExceptions(RetryableException.class);

        errorHandler.addNotRetryableExceptions(
                SerializationException.class,
                RestClientException.class
        );

        factory.setCommonErrorHandler(errorHandler);

        return factory;
    }

    @Bean
    KafkaTemplate<String, Device> kafkaTemplate(ProducerFactory<String, Device> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    ProducerFactory<String, Device> producerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                environment.getProperty("spring.kafka.bootstrap-servers")
        );
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        config.put(
                "schema.registry.url",
                environment.getProperty("spring.kafka.producer.properties.schema.registry.url")
        );

        return new DefaultKafkaProducerFactory<>(config);
    }

}
