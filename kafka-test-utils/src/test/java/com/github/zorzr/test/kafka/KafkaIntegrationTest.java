package com.github.zorzr.test.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class KafkaIntegrationTest {
    private static final KafkaLocalInstance kafkaLocalInstance = KafkaLocalInstance.initInstance();
    private static final String TOPIC = "TEST-TOPIC";

    @BeforeAll
    public static void initKafka() {
        kafkaLocalInstance.start();
        kafkaLocalInstance.createTopic(TOPIC);
    }

    @AfterAll
    public static void closeKafka() {
        kafkaLocalInstance.deleteAllTopics();
        kafkaLocalInstance.close();
    }


    @Test
    public void testKafkaA() {
        // Publish records to Kafka and consume from earliest
        RecordHeaders headers = new RecordHeaders(new RecordHeader[]{new RecordHeader("test-header", "value".getBytes())});
        ProducerRecord<byte[],byte[]> recordA = kafkaLocalInstance.producerRecord(TOPIC, "A".getBytes(), "{\"key\":\"A\"}".getBytes());
        ProducerRecord<byte[],byte[]> recordB = kafkaLocalInstance.producerRecord(TOPIC, "B".getBytes(), "{\"key\":\"B\"}".getBytes(), headers);
        kafkaLocalInstance.produce(recordA);
        kafkaLocalInstance.produce(recordB);

        List<ConsumerRecord<byte[],byte[]>> consumerRecords = kafkaLocalInstance.consume(TOPIC);
        assertEquals(2, consumerRecords.size());
        assertEquals(new String(recordA.value()), new String(consumerRecords.get(0).value()));
        assertEquals(new String(recordB.value()), new String(consumerRecords.get(1).value()));
        assertEquals("value", new String(consumerRecords.get(1).headers().lastHeader("test-header").value()));
    }

    @Test
    public void testKafkaB() {
        // Same topic, to avoid recreating it we can just use a consumer for latest records
        KafkaConsumer<byte[],byte[]> consumer = kafkaLocalInstance.kafkaLatestConsumerForTopic(TOPIC);
        ProducerRecord<byte[],byte[]> recordC = kafkaLocalInstance.producerRecord(TOPIC, "C".getBytes(), "{\"key\":\"C\"}".getBytes());
        kafkaLocalInstance.produce(recordC);

        List<ConsumerRecord<byte[],byte[]>> consumerRecords = kafkaLocalInstance.consume(consumer, TOPIC, Duration.ofSeconds(3));
        assertEquals(1, consumerRecords.size());
        assertEquals(new String(recordC.value()), new String(consumerRecords.get(0).value()));
    }
}
