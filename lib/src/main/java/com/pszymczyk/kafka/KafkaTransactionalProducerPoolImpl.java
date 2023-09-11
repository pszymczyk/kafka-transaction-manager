package com.pszymczyk.kafka;

import com.pszymczyk.kafka.api.KafkaTransactionalProducerPool;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class KafkaTransactionalProducerPoolImpl<K, V> implements KafkaTransactionalProducerPool<K, V>, AutoCloseable{

    private final BlockingQueue<PoolAwareTransactionalProducer<K, V>> cache;

    public KafkaTransactionalProducerPoolImpl(Map<String, Object> configs,
                                       Serializer<K> keySerializer,
                                       Serializer<V> valueSerializer,
                                       String txIdPrefix,
                                       int poolSize) {
        this.cache = new ArrayBlockingQueue<>(poolSize);
        init(configs, keySerializer, valueSerializer, txIdPrefix, poolSize);
    }

    private void init(Map<String, Object> configs, Serializer<K> keySerializer, Serializer<V> valueSerializer, String txIdPrefix, int poolSize) {
        for (int i = 0; i < poolSize; i++) {
            Map<String, Object> mutableConfigs = new HashMap<>(configs);
            mutableConfigs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, txIdPrefix+"-"+i);
            PoolAwareTransactionalProducer<K, V> producer = new PoolAwareTransactionalProducer<>(new KafkaProducer<>(mutableConfigs, keySerializer, valueSerializer), this);
            cache.add(producer);
        }
    }


    @Override
    public Producer<K, V> getTransactionalProducer() {
        return cache.poll();
    }

    void release(PoolAwareTransactionalProducer<K,V> poolAwareTransactionalProducer) {
        cache.add(poolAwareTransactionalProducer);
    }

    @Override
    public void close() throws Exception {
        cache.forEach(PoolAwareTransactionalProducer::closeDelegate);
    }
}
