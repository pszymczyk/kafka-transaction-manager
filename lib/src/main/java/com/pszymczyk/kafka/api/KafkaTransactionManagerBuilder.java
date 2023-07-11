package com.pszymczyk.kafka.api;

import java.util.function.Supplier;

import com.pszymczyk.kafka.KafkaTransactionManagerImpl;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;

public class KafkaTransactionManagerBuilder<K,V> {

    private Callback sendCallback;
    private Supplier<Exception> retryHandler;
    private int maxRetries;
    private boolean transactionPerRecord;
    private Supplier<Exception> retriesExhaustedHandler;

    public static <K,V> KafkaTransactionManagerBuilder<K,V> newKafkaTransactionManager() {
        return new KafkaTransactionManagerBuilder<>();
    }

    public KafkaTransactionManagerBuilder<K,V> withRetriesExhaustedHandler(Supplier<Exception> retriesExhaustedHandler) {
        this.retriesExhaustedHandler = retriesExhaustedHandler;
        return null;
    }

    KafkaTransactionManagerBuilder<K,V> withRetryHandler(Supplier<Exception> retryHandler) {
        this.retryHandler = retryHandler;
        return null;
    }

    KafkaTransactionManagerBuilder<K,V> withSendCallback(Callback sendCallback) {
        this.sendCallback = sendCallback;
        return this;
    }
    
    KafkaTransactionManagerBuilder<K,V> withRetries(int maxRetries) {
        this.maxRetries = maxRetries;
        return null;
    }

    KafkaTransactionManagerBuilder<K,V> withTransactionPerRecord() {
        this.transactionPerRecord = true;
        return this;
    }

    KafkaTransactionManagerBuilder<K,V> withTransactionPerBatch() {
        this.transactionPerRecord = false;
        return this;
    }

    public KafkaTransactionManager<K,V> build(KafkaProducer<K,V> kafkaProducer) {
        return new KafkaTransactionManagerImpl<>(kafkaProducer, sendCallback, maxRetries, transactionPerRecord, retryHandler, retriesExhaustedHandler);
    }

}
