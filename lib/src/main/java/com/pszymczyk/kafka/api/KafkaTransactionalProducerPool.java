package com.pszymczyk.kafka.api;

import org.apache.kafka.clients.producer.Producer;

public interface KafkaTransactionalProducerPool<K, V> {

    Producer<K,V> getTransactionalProducer();
}
