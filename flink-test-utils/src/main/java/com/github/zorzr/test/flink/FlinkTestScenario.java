package com.github.zorzr.test.flink;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FlinkTestScenario {
    private Map<String, List<String>> producer;
    private Map<String, List<String>> consumer;
    private DatabaseSetupOptions database;

    @JsonIgnore
    private final Map<String, KafkaConsumer<byte[],byte[]>> listeners = new HashMap<>();

    public Map<String, List<String>> getProducer() {
        return producer;
    }

    public void setProducer(Map<String, List<String>> producer) {
        this.producer = producer;
    }

    public Map<String, List<String>> getConsumer() {
        return consumer;
    }

    public void setConsumer(Map<String, List<String>> consumer) {
        this.consumer = consumer;
    }

    public DatabaseSetupOptions getDatabase() {
        return database;
    }

    public void setDatabase(DatabaseSetupOptions database) {
        this.database = database;
    }

    public Map<String, KafkaConsumer<byte[], byte[]>> getListeners() {
        return listeners;
    }

    public void registerConsumer(String topic, KafkaConsumer<byte[], byte[]> consumer) {
        listeners.put(topic, consumer);
    }

    static class DatabaseSetupOptions {
        private List<String> initialization;
        private List<String> collection;

        public List<String> getInitialization() {
            return initialization;
        }

        public void setInitialization(List<String> initialization) {
            this.initialization = initialization;
        }

        public List<String> getCollection() {
            return collection;
        }

        public void setCollection(List<String> collection) {
            this.collection = collection;
        }
    }
}
