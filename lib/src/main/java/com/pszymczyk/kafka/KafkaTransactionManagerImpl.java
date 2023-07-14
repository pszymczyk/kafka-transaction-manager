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

import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class KafkaTransactionManagerImpl<PRK, PRV, CRK, CRV> implements KafkaTransactionManager<CRK, CRV> {

    private final KafkaProducer<PRK, PRV> kafkaProducer;
    private final ConsumerGroupMetadata consumerGroupMetadata;
    private final Function<ConsumerRecord<CRK, CRV>, List<ProducerRecord<PRK, PRV>>> transactionPerRecord;
    private final Callback sendCallback;
    private final int maxRetries;
    private final Consumer<Exception> retryHandler;
    private final Consumer<ConsumerRecord<CRK, CRV>> retryExhaustedHandler;

    public KafkaTransactionManagerImpl(KafkaProducer<PRK, PRV> kafkaProducer, ConsumerGroupMetadata consumerGroupMetadata, Function<ConsumerRecord<CRK, CRV>, List<ProducerRecord<PRK, PRV>>> transactionPerRecord, Callback sendCallback, int maxRetries, Consumer<Exception> retryHandler, Consumer<ConsumerRecord<CRK, CRV>> retriesExhaustedHandler) {
        this.kafkaProducer = kafkaProducer;
        this.consumerGroupMetadata = consumerGroupMetadata;
        this.transactionPerRecord = transactionPerRecord;
        this.sendCallback = sendCallback;
        this.maxRetries = maxRetries;
        this.retryHandler = retryHandler;
        this.retryExhaustedHandler = retriesExhaustedHandler;
        this.kafkaProducer.initTransactions();
    }


    @Override
    public List<Future<RecordMetadata>> handleInTransaction(ConsumerRecords<CRK, CRV> consumerRecords) {
        int retriesCounter = 0;
        try {
            for (var consumerRecord : consumerRecords) {
                if (retriesCounter < maxRetries) {
                    kafkaProducer.beginTransaction();
                    List<ProducerRecord<PRK, PRV>> producerRecords = transactionPerRecord.apply(consumerRecord);
                    List<Future<RecordMetadata>> collect = producerRecords.stream().map(pR -> kafkaProducer.send(pR, sendCallback)).collect(Collectors.toList());
                    kafkaProducer.sendOffsetsToTransaction(Map.of(new TopicPartition(consumerRecord.topic(), consumerRecord.partition()), new OffsetAndMetadata(consumerRecord.offset())), consumerGroupMetadata);
                    kafkaProducer.commitTransaction();

                    return collect;
                } else {
                    //TODO retry exhausted
                    return null;
                }
            }

        } catch (Exception e) {
            ++retriesCounter;
            kafkaProducer.abortTransaction();
        }
        return null;
    }
}
