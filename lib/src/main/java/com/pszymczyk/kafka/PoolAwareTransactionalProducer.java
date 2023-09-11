package com.pszymczyk.kafka;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

public class PoolAwareTransactionalProducer<K,V> implements Producer<K,V> {

    private final KafkaProducer<K,V> kafkaProducer;
    private final KafkaTransactionalProducerPoolImpl<K,V> kafkaTransactionalProducerPool;

    public PoolAwareTransactionalProducer(KafkaProducer<K, V> kafkaProducer, KafkaTransactionalProducerPoolImpl<K,V> kafkaTransactionalProducerPool) {
        this.kafkaProducer = kafkaProducer;
        this.kafkaTransactionalProducerPool = kafkaTransactionalProducerPool;
    }

    @Override
    public void initTransactions() {
        kafkaProducer.initTransactions();
    }

    @Override
    public void beginTransaction() throws ProducerFencedException {
        kafkaProducer.beginTransaction();
    }

    @Override
    @Deprecated
    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, String consumerGroupId) throws ProducerFencedException {
        kafkaProducer.sendOffsetsToTransaction(offsets, consumerGroupId);
    }

    @Override
    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, ConsumerGroupMetadata groupMetadata) throws ProducerFencedException {
        kafkaProducer.sendOffsetsToTransaction(offsets, groupMetadata);
    }

    @Override
    public void commitTransaction() throws ProducerFencedException {
        kafkaProducer.commitTransaction();
    }

    @Override
    public void abortTransaction() throws ProducerFencedException {
        kafkaProducer.abortTransaction();
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
        return kafkaProducer.send(record);
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
        return kafkaProducer.send(record, callback);
    }

    @Override
    public void flush() {
        kafkaProducer.flush();
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return kafkaProducer.partitionsFor(topic);
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return kafkaProducer.metrics();
    }

    @Override
    public void close() {
        kafkaTransactionalProducerPool.release(this);
    }

    @Override
    public void close(Duration timeout) {
        kafkaTransactionalProducerPool.release(this);
    }

    void closeDelegate() {
        kafkaProducer.close();
    }
}
