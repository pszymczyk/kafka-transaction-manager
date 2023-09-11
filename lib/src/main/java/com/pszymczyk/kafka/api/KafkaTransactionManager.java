package com.pszymczyk.kafka.api;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.List;
import java.util.concurrent.Future;

public interface KafkaTransactionManager<CRK, CRV> {

    List<Future<RecordMetadata>> executeInTransaction(ConsumerRecords<CRK, CRV> consumerRecords);

}
