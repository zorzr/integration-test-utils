package com.github.zorzr.test.kafka;

import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class KafkaLocalInstance {
    private static final Logger logger = LoggerFactory.getLogger(KafkaLocalInstance.class);
    public static final String localGroupID = "KAFKA-LOCAL-CONSUMER-GROUP-ID";

    private static KafkaLocalInstance kafkaLocalInstance;
    private static SharedKafkaTestResource sharedKafkaResource;
    private static boolean started = false;

    private final int brokersNumber;
    private final Properties brokerProperties;

    private KafkaLocalInstance(int brokers, Properties brokerConfig) {
        brokersNumber = brokers;
        brokerProperties = brokerConfig;
        initCluster();
    }

    private void initCluster() {
        sharedKafkaResource = new SharedKafkaTestResource(brokerProperties)
                .withBrokers(brokersNumber);
    }

    public static KafkaLocalInstance initInstance() {
        return initInstance(1, new Properties());
    }

    public static KafkaLocalInstance initInstance(int brokers, Properties brokerConfig) {
        if (kafkaLocalInstance != null && started) {
            kafkaLocalInstance.close();
        }
        kafkaLocalInstance = new KafkaLocalInstance(brokers, brokerConfig);
        return kafkaLocalInstance;
    }

    public static KafkaLocalInstance getInstance() {
        // Singleton instance
        return kafkaLocalInstance;
    }

    public void start() {
        try {
            sharedKafkaResource.beforeAll(null);
            started = true;
        } catch (Exception e) {
            logger.error("Exception caught while starting Kafka instance", e);
            throw new RuntimeException(e);
        }
    }

    public void close() {
        try {
            sharedKafkaResource.afterAll(null);
            sharedKafkaResource = null;
            started = false;
        } catch (Exception e) {
            logger.error("Exception caught while closing Kafka instance", e);
            throw new RuntimeException(e);
        }
    }

    @Deprecated
    public void refresh() {
        // On Windows there could be errors when deleting topics
        // The only way to recreate a topic is to close the instance and start a new one
        // Keep in mind that this is time-consuming and leaves zombie files in the Temp directory!
        // A smarter approach is to avoid topic deletion and consume from latest
        close();
        initCluster();
        start();
    }

    public void createTopic(String name) {
        createTopic(name, 1, 1);
    }
    public void createTopic(String name, int partitions, int replication) {
        sharedKafkaResource.getKafkaTestUtils()
                .createTopic(name, partitions, (short) replication);
    }
    public void createTopics(String... topics) {
        Arrays.stream(topics).forEach(t -> createTopic(t, 1, 1));
    }
    public void createTopics(Set<NewTopic> newTopics) {
        newTopics.forEach(t ->
                sharedKafkaResource.getKafkaTestUtils()
                        .createTopic(t.name(), t.numPartitions(), t.replicationFactor()));

    }

    public void deleteTopics(Set<String> topics) {
        AdminClient adminClient = sharedKafkaResource.getKafkaTestUtils().getAdminClient();
        adminClient.deleteTopics(topics);
    }

    public void deleteAllTopics() {
        AdminClient adminClient = sharedKafkaResource.getKafkaTestUtils().getAdminClient();
        adminClient.deleteTopics(sharedKafkaResource.getKafkaTestUtils().getTopicNames());
    }


    public <K, V> ProducerRecord<K, V> producerRecord(String topic, K key, V value) {
        return new ProducerRecord<>(topic, key, value);
    }
    public <K, V> ProducerRecord<K, V> producerRecord(String topic, K key, V value, Iterable<Header> headers) {
        return new ProducerRecord<>(topic, null, key, value, headers);
    }

    public void produce(ProducerRecord<byte[],byte[]> producerRecord) {
        produce(kafkaProducer(), producerRecord);
    }
    public <K, V> void produce(KafkaProducer<K, V> kafkaProducer, ProducerRecord<K, V> producerRecord) {
        try {
            kafkaProducer.send(producerRecord).get();
        } catch (Exception e) {
            logger.error("Exception caught while publishing record", e);
            throw new RuntimeException(e);
        }
    }

    public List<ConsumerRecord<byte[],byte[]>> consume(String topic) {
        return kafkaLocalInstance.getSharedKafkaResource().getKafkaTestUtils().consumeAllRecordsFromTopic(topic);
    }
    public List<ConsumerRecord<byte[],byte[]>> consume(String topic, Duration duration) {
        return consume(kafkaConsumer(), topic, duration);
    }
    public List<ConsumerRecord<byte[],byte[]>> consume(String topic, Duration duration, int limit) {
        return consume(kafkaConsumer(), topic, duration, limit);
    }
    public <K, V> List<ConsumerRecord<K, V>> consume(KafkaConsumer<K, V> kafkaConsumer, String topic, Duration duration) {
        return consume(kafkaConsumer, topic, duration, Integer.MAX_VALUE);
    }
    public <K, V> List<ConsumerRecord<K, V>> consume(KafkaConsumer<K, V> kafkaConsumer, String topic, Duration duration, int limit) {
        try {
            List<ConsumerRecord<K, V>> consumerRecords = new ArrayList<>();
            if (!kafkaConsumer.subscription().contains(topic)) {
                kafkaConsumer.subscribe(Collections.singleton(topic));
            }

            Instant start = Instant.now();
            long timeout = duration.toNanos();
            while (consumerRecords.size() < limit && checkTimeout(start, timeout)) {
                ConsumerRecords<K, V> records = kafkaConsumer.poll(Duration.ofSeconds(1));
                records.forEach(consumerRecords::add);
            }

            return consumerRecords;
        } catch (Exception e) {
            logger.error("Exception caught while consuming records", e);
            throw new RuntimeException(e);
        }
    }

    public boolean checkTimeout(Instant start, long timeout) {
        Duration elapsed = Duration.between(start, Instant.now());
        return elapsed.toNanos() < timeout;
    }

    public KafkaProducer<byte[], byte[]> kafkaProducer() {
        return kafkaProducer(ByteArraySerializer.class, ByteArraySerializer.class);
    }
    public <K, V> KafkaProducer<K, V> kafkaProducer(Class<? extends Serializer<K>> keySerializer,
                                                    Class<? extends Serializer<V>> valueSerializer) {
        return sharedKafkaResource.getKafkaTestUtils().getKafkaProducer(keySerializer, valueSerializer);
    }

    public KafkaConsumer<byte[], byte[]> kafkaConsumer() {
        return kafkaConsumer(ByteArrayDeserializer.class, ByteArrayDeserializer.class);
    }
    public <K, V> KafkaConsumer<K, V> kafkaConsumer(Class<? extends Deserializer<K>> keyDeserializer,
                                                    Class<? extends Deserializer<V>> valueDeserializer) {
        Properties config = new Properties();
        config.put("group.id", localGroupID);
        config.put("auto.offset.reset", "earliest");
        config.put("enable.auto.commit", true);
        return sharedKafkaResource.getKafkaTestUtils().getKafkaConsumer(keyDeserializer, valueDeserializer, config);
    }

    public KafkaConsumer<byte[], byte[]> kafkaLatestConsumerForTopic(String topic) {
        Properties config = new Properties();
        config.put("group.id", localGroupID);
        config.put("auto.offset.reset", "latest");
        config.put("enable.auto.commit", true);
        KafkaConsumer<byte[], byte[]> kafkaConsumer = sharedKafkaResource.getKafkaTestUtils()
                .getKafkaConsumer(ByteArrayDeserializer.class, ByteArrayDeserializer.class, config);
        kafkaConsumer.subscribe(Collections.singleton(topic));
        while (kafkaConsumer.assignment().isEmpty()) {
            kafkaConsumer.poll(Duration.ofMillis(100L));
        }
        return kafkaConsumer;
    }

    public void commitLatestOffsetForGroup(String topic, String groupId) {
        Properties config = new Properties();
        config.put("group.id", groupId);
        config.put("auto.offset.reset", "latest");
        config.put("enable.auto.commit", true);
        KafkaConsumer<byte[], byte[]> kafkaConsumer = sharedKafkaResource.getKafkaTestUtils()
                .getKafkaConsumer(ByteArrayDeserializer.class, ByteArrayDeserializer.class, config);
        kafkaConsumer.subscribe(Collections.singleton(topic));
        while (kafkaConsumer.assignment().isEmpty()) {
            kafkaConsumer.poll(Duration.ofMillis(100L));
        }
        kafkaConsumer.close();
    }

    public SharedKafkaTestResource getSharedKafkaResource() {
        return sharedKafkaResource;
    }

    public String getBootstrapServers() {
        return sharedKafkaResource.getKafkaConnectString();
    }
}
