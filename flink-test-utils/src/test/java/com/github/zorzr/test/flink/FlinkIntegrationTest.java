package com.github.zorzr.test.flink;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.zorzr.test.kafka.KafkaLocalInstance;
import com.schibsted.spt.data.jslt.Expression;
import com.schibsted.spt.data.jslt.Parser;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static com.github.zorzr.test.flink.TestUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class FlinkIntegrationTest extends BaseFlinkTest {
    private static final KafkaLocalInstance kafkaLocalInstance = KafkaLocalInstance.initInstance();

    private static final Expression jslt = Parser.compileString(loadResource("data/Transformer.jslt"));
    private static final String SOURCE_TOPIC = "SOURCE-TOPIC";
    private static final String SINK_TOPIC = "SINK-TOPIC";

    @BeforeAll
    public static void initKafka() {
        kafkaLocalInstance.start();
        kafkaLocalInstance.createTopics(SOURCE_TOPIC, SINK_TOPIC);
    }

    @AfterAll
    public static void closeKafka() {
        kafkaLocalInstance.deleteAllTopics();
        kafkaLocalInstance.close();
    }

    @BeforeEach
    public void checkEnvironment() {
        // Shared environment among all test cases, executed only once
        checkDefaultExecution();
    }

    private KafkaSource<String> kafkaSource() {
        return KafkaSource.<String>builder()
                .setBootstrapServers(kafkaLocalInstance.getBootstrapServers())
                .setTopics(SOURCE_TOPIC)
                .setGroupId("FLINK-GROUP-ID")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }

    private KafkaSink<String> kafkaSink() {
        return KafkaSink.<String>builder()
                .setBootstrapServers(kafkaLocalInstance.getBootstrapServers())
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .setTopic(SINK_TOPIC)
                        .build())
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }

    public void setupEnvironment(StreamExecutionEnvironment env) {
        DataStream<String> input = env.fromSource(kafkaSource(), WatermarkStrategy.noWatermarks(), "KafkaSource");
        DataStream<String> parsed = input.flatMap((String s, Collector<String> collector) -> {
            JsonNode node = jsonToTree(s);
            if (!node.isEmpty()) {
                JsonNode transformed = jslt.apply(node);
                collector.collect(transformed.toString());
            }
        }).returns(String.class);
        parsed.sinkTo(kafkaSink());
    }


    public FlinkTestScenario initScenario(String path) {
        FlinkTestScenario scenario = loadTest(path);
        scenario.getConsumer().keySet().forEach(topic -> {
            KafkaConsumer<byte[], byte[]> consumer = kafkaLocalInstance.kafkaLatestConsumerForTopic(topic);
            scenario.registerConsumer(topic, consumer);
        });
        scenario.getProducer().forEach((topic, events) ->
            events.forEach(payload -> {
                ProducerRecord<byte[],byte[]> producerRecord = kafkaLocalInstance.producerRecord(topic, "".getBytes(), payload.getBytes());
                kafkaLocalInstance.produce(producerRecord);
            })
        );
        return scenario;
    }

    public void evaluateScenario(FlinkTestScenario scenario) {
        scenario.getListeners().forEach((topic, consumer) -> {
            List<ConsumerRecord<byte[],byte[]>> consumerRecords = kafkaLocalInstance.consume(consumer, topic, Duration.ofSeconds(1));
            List<String> expectedValues = scenario.getConsumer().get(topic);
            assertEquals(expectedValues.size(), consumerRecords.size());
            for (int i = 0; i < expectedValues.size(); i++) {
                JsonNode expected = jsonToTree(expectedValues.get(i));
                JsonNode actual = jsonToTree(new String(consumerRecords.get(i).value()));
                assertEquals(expected, actual);
            }
            consumer.unsubscribe();
        });
    }

    @Test
    public void testFlinkA() {
        FlinkTestScenario scenario = initScenario("data/Test_A.json");
        evaluateScenario(scenario);
    }

    @Test
    public void testFlinkB() {
        FlinkTestScenario scenario = initScenario("data/Test_B.json");
        evaluateScenario(scenario);
    }
}
