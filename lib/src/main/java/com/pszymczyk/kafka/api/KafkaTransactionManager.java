package com.pszymczyk.kafka.api;

import java.util.List;
import java.util.concurrent.Future;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

//THREAD safe supprt todo
public interface KafkaTransactionManager<PRK, PRV> {

    <CRK, CRV> List<Future<RecordMetadata>> readProcessWrite(ConsumerRecords<CRK, CRV> consumerRecords, ConsumerGroupMetadata consumerGroupMetadata, Function<ConsumerRecords<CRK, CRV>, List<ProducerRecord<PRK, PRV>>> f);

    List<Future<RecordMetadata>> sendInTransaction(List<ProducerRecord<PRK, PRV>> records);
}
