package com.pszymczyk.kafka.api;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

public interface KafkaTransactionManager<CRK, CRV> {

    Map<ConsumerRecord<CRK, CRV>, List<Future<RecordMetadata>>> handleInTransaction(ConsumerRecords<CRK, CRV> consumerRecords);

}
