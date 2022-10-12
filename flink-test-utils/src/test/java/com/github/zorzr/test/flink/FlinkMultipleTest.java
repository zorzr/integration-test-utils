package com.github.zorzr.test.flink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.zorzr.test.kafka.KafkaLocalInstance;
import com.github.zorzr.test.mongo.MongoLocalInstance;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
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
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.bson.Document;
import org.junit.jupiter.api.*;
import org.mongoflink.config.MongoConnectorOptions;
import org.mongoflink.sink.MongoSink;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

import static com.github.zorzr.test.flink.TestUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class FlinkMultipleTest extends BaseFlinkTest {
    private static final KafkaLocalInstance kafkaLocalInstance = KafkaLocalInstance.initInstance();
    private static final MongoLocalInstance mongoLocalInstance = MongoLocalInstance.defaultInstance();

    private static final Expression jslt = Parser.compileString(loadResource("data/Transformer.jslt"));
    private static final String SOURCE_TOPIC = "SOURCE-TOPIC";
    private static final String SINK_TOPIC = "SINK-TOPIC";

    private static final String TEST_DB = "testDB";
    private static final String SINK_COLLECTION = "sink";
    private static MongoClient mongoClient;

    @BeforeAll
    public static void initKafka() {
        kafkaLocalInstance.start();
        kafkaLocalInstance.createTopics(SOURCE_TOPIC, SINK_TOPIC);
    }
    @BeforeAll
    public static void initMongo() {
        mongoLocalInstance.start();
        mongoClient = mongoLocalInstance.getMongoClient();
    }

    @AfterAll
    public static void closeKafka() {
        kafkaLocalInstance.deleteAllTopics();
        kafkaLocalInstance.close();
    }

    @AfterEach
    public void alignOffsets() {
        // New environment for each test (possibly different)
        // Make sure that each consumer fetches only the latest relevant events
        kafkaLocalInstance.commitLatestOffsetForGroup(SOURCE_TOPIC, "FLINK-GROUP-ID");
    }


    private KafkaSource<String> kafkaSource() {
        return KafkaSource.<String>builder()
                .setBootstrapServers(kafkaLocalInstance.getBootstrapServers())
                .setTopics(SOURCE_TOPIC)
                .setGroupId("FLINK-GROUP-ID")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
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

    private MongoSink<String> mongoSink() {
        MongoConnectorOptions options = MongoConnectorOptions.builder()
                .withDatabase(TEST_DB)
                .withCollection(SINK_COLLECTION)
                .withConnectString(mongoLocalInstance.getConnectionString())
                .withUpsertEnable(true)
                .withUpsertKey(new String[]{"userId"})
                .withFlushInterval(Duration.ofSeconds(1L))
                .withFlushSize(1)
                .build();
        return new MongoSink<>(Document::parse, options);
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

    public boolean evaluateScenario(FlinkTestScenario scenario) {
        scenario.getListeners().forEach((topic, consumer) -> {
            List<ConsumerRecord<byte[],byte[]>> consumerRecords = kafkaLocalInstance.consume(consumer, topic, Duration.ofSeconds(2));
            List<String> expectedValues = scenario.getConsumer().get(topic);
            assertEquals(expectedValues.size(), consumerRecords.size());
            for (int i = 0; i < expectedValues.size(); i++) {
                JsonNode expected = jsonToTree(expectedValues.get(i));
                JsonNode actual = jsonToTree(new String(consumerRecords.get(i).value()));
                assertEquals(expected, actual);
            }
            consumer.unsubscribe();
        });
        return true;
    }

    @Test
    public void testFlinkA() {
        FlinkTestScenario scenario = initScenario("data/Test_A.json");
        runTest(() -> evaluateScenario(scenario));
    }

    @Test
    public void testFlinkC() {
        // The runTest() method allows receiving a custom environment definition
        // By default, it takes the environment definition in the setupEnvironment() method
        FlinkTestScenario scenario = initScenario("data/Test_C.json");

        // Custom environment
        runTest(env -> {
            DataStream<String> input = env.fromSource(kafkaSource(), WatermarkStrategy.noWatermarks(), "KafkaSource");
            DataStream<String> parsed = input.flatMap((String s, Collector<String> collector) -> {
                JsonNode node = jsonToTree(s);
                if (!node.isEmpty()) {
                    JsonNode transformed = jslt.apply(node);
                    collector.collect(transformed.toString());
                }
            }).returns(String.class);
            parsed.sinkTo(mongoSink());
        },

        // Evaluation function
        () -> {
            MongoDatabase database = mongoClient.getDatabase(TEST_DB);
            MongoCollection<Document> collection = database.getCollection(SINK_COLLECTION);
            while (collection.countDocuments() == scenario.getDatabase().getInitialization().size()) sleep(200L);
            scenario.getDatabase().getRecords().forEach(record -> {
                JsonNode expected = jsonToTree(record);
                Document result = collection.find(new Document("userId", expected.get("userId").asText())).first();
                JsonNode actual = jsonToTree(Objects.requireNonNull(result).toJson());
                ((ObjectNode) actual).remove("_id");
                assertEquals(expected, actual);
            });
            return true;
        });
    }
}
