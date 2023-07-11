package com.pszymczyk.kafka;

import com.pszymczyk.kafka.api.KafkaTransactionManager;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class KafkaTransactionManagerImpl<PRK, PRV> implements KafkaTransactionManager<PRK, PRV> {

    private final KafkaProducer<PRK, PRV> kafkaProducer;
    private final Callback sendCallback;
    private final int maxRetries;
    private final boolean transactionPerRecord;
    private final Supplier<Exception> retryHandler;
    private final Supplier<ConsumerRecord> retryExhaustedHandler;

    public KafkaTransactionManagerImpl(KafkaProducer<PRK, PRV> kafkaProducer, Callback sendCallback, int maxRetries, boolean transactionPerRecord, Supplier<Exception> retryHandler, Supplier<ConsumerRecord<K,V>> retryExhaustedHandler) {
        this.kafkaProducer = kafkaProducer;
        this.sendCallback = sendCallback;
        this.maxRetries = maxRetries;
        this.transactionPerRecord = transactionPerRecord;
        this.retryHandler = retryHandler;
        this.retryExhaustedHandler = retryExhaustedHandler;
    }

    @Override
    public List<Future<RecordMetadata>> sendInTransaction(List<ProducerRecord<PRK, PRV>> producerRecords) {
        return null;
    }

    @Override
    public <CRK, CRV> List<Future<RecordMetadata>> readProcessWrite(ConsumerRecords<CRK, CRV> consumerRecords, ConsumerGroupMetadata consumerGroupMetadata, Function<ConsumerRecords<CRK, CRV>, List<ProducerRecord<PRK, PRV>>> f) {
        int retriesCounter = 0;
        try {
            if (retriesCounter < maxRetries) {
                kafkaProducer.beginTransaction();
                List<ProducerRecord<PRK, PRV>> producerRecords = f.apply(consumerRecords);
                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                consumerRecords.forEach(cR -> offsets.put(new TopicPartition(cR.topic(), cR.partition()), new OffsetAndMetadata(cR.offset())));
                kafkaProducer.sendOffsetsToTransaction(offsets, consumerGroupMetadata);
                List<Future<RecordMetadata>> collect = producerRecords.stream().map(pR -> kafkaProducer.send(pR, sendCallback)).collect(Collectors.toList());
                kafkaProducer.commitTransaction();
                return collect;
            } else {
                retryExhaustedHandler.
            }

        } catch (Exception e) {
            retriesCounter++;
            kafkaProducer.abortTransaction();
        }
        return null;
    }
}
