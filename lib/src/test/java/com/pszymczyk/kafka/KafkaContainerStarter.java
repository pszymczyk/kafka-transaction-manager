package com.pszymczyk.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

import static java.util.Objects.requireNonNull;

class KafkaContainerStarter {

    static KafkaContainer kafkaContainer;

    static void start() {
        if (kafkaContainer == null) {
            kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0")).withKraft();
            kafkaContainer.start();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> kafkaContainer.close()));
            bootstrapMessages();
        }
    }

    private static void bootstrapMessages() {
        KafkaProducer<String, String> kafkaProducer = kafkaProducer();
        requireNonNull(BootstrapMessages.getBootstrapMessages()).stream()
                .map(line -> line.split(":"))
                .map(line -> new ProducerRecord<String, String>(line[0], line[1]))
                .forEach(producerRecord -> {
                    try {
                        kafkaProducer.send(producerRecord).get();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
        kafkaProducer.close();
    }

    static String getBootstrapServers() {
        return kafkaContainer.getBootstrapServers();
    }

    protected static KafkaProducer<String, String> kafkaProducer() {
        return new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class));
    }
}
