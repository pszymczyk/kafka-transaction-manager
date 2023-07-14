package com.pszymczyk.kafka.api;

import com.pszymczyk.kafka.KafkaTransactionManagerImpl;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

public class KafkaTransactionManagerBuilder<PRK, PRV, CRK, CRV> {

    private Callback sendCallback;
    private int maxRetries;
    private Function<ConsumerRecord<CRK, CRV>, List<ProducerRecord<PRK, PRV>>> transactionPerRecord;
    private Consumer<Exception> retryHandler;
    private Consumer<ConsumerRecord<CRK,CRV>> retriesExhaustedHandler;
    private ConsumerGroupMetadata consumerGroupMetadata;

    public static <PRK, PRV, CRK, CRV> KafkaTransactionManagerBuilder<PRK, PRV, CRK, CRV> newKafkaTransactionManager() {
        return new KafkaTransactionManagerBuilder<>();
    }

    public KafkaTransactionManagerBuilder<PRK, PRV, CRK, CRV> withRetriesExhaustedHandler(Consumer<ConsumerRecord<CRK, CRV>> retriesExhaustedHandler) {
        this.retriesExhaustedHandler = retriesExhaustedHandler;
        return null;
    }

    public KafkaTransactionManagerBuilder<PRK, PRV, CRK, CRV> withRetryHandler(Consumer<Exception> retryHandler) {
        this.retryHandler = retryHandler;
        return null;
    }

    public KafkaTransactionManagerBuilder<PRK, PRV, CRK, CRV> withSendCallback(Callback sendCallback) {
        this.sendCallback = sendCallback;
        return this;
    }
    
    public KafkaTransactionManagerBuilder<PRK, PRV, CRK, CRV> withRetries(int maxRetries) {
        this.maxRetries = maxRetries;
        return this;
    }

    public KafkaTransactionManagerBuilder<PRK, PRV, CRK, CRV> withTransactionPerRecord(Function<ConsumerRecord<CRK, CRV>, List<ProducerRecord<PRK, PRV>>> handler) {
        this.transactionPerRecord = handler;
        return this;
    }

    //TODO
//    public KafkaTransactionManagerBuilder<PRK, PRV, CRK, CRV> withTransactionPerBatch(Function<ConsumerRecords<CRK, CRV>, List<ProducerRecord<String, String>>> handler) {
//        this.transactionPerRecord = false;
//        return this;
//    }

    public KafkaTransactionManagerBuilder<PRK, PRV, CRK, CRV> withConsumerGroupMetadata(ConsumerGroupMetadata consumerGroupMetadata) {
        this.consumerGroupMetadata = consumerGroupMetadata;
        return this;
    }

    public KafkaTransactionManager<CRK, CRV> build(KafkaProducer<PRK, PRV> kafkaProducer) {
        return new KafkaTransactionManagerImpl<>(kafkaProducer,
                consumerGroupMetadata,
                transactionPerRecord,
                sendCallback,
                maxRetries,
                retryHandler,
                retriesExhaustedHandler);
    }

}
