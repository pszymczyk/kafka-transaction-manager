package com.pszymczyk.kafka.api;

import com.pszymczyk.kafka.KafkaTransactionalProducerPoolImpl;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class KafkaTransactionalProducerPoolBuilder<K,V> {

    private Map<String, Object> configs;
    private Serializer<K> keySerializer;
    private Serializer<V> valueSerializer;
    private String txIdPrefix;
    private int poolSize = 1;

    public KafkaTransactionalProducerPoolBuilder<K,V> withConfigs(Map<String, Object> configs) {
        this.configs = configs;
        return this;
    }

    public KafkaTransactionalProducerPoolBuilder<K,V> withKeySerializer(Serializer<K> keySerializer) {
        this.keySerializer = keySerializer;
        return this;
    }

    public KafkaTransactionalProducerPoolBuilder<K,V> withValueSerializer(Serializer<V> valueSerializer) {
        this.valueSerializer = valueSerializer;
        return this;
    }

    public KafkaTransactionalProducerPoolBuilder<K,V> withTransactionalIdPrefix(String txIdPrefix) {
        this.txIdPrefix = txIdPrefix;
        return this;
    }

    public KafkaTransactionalProducerPoolBuilder<K,V> withPoolSize(int poolSize) {
        this.poolSize = poolSize;
        return this;
    }

    public KafkaTransactionalProducerPool<K,V> build() {
        requireNonNull(configs);
        requireNonNull(txIdPrefix);
        return new KafkaTransactionalProducerPoolImpl<>(configs, keySerializer, valueSerializer, txIdPrefix, poolSize);
    }
}
