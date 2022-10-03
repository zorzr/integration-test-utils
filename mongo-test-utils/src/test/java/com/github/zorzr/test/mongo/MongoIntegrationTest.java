package com.github.zorzr.test.mongo;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class MongoIntegrationTest {
    private static final MongoLocalInstance mongoLocalInstance = MongoLocalInstance.defaultInstance();
    private static MongoClient mongoClient;

    @BeforeAll
    public static void initMongo() {
        mongoLocalInstance.start();
        mongoClient = mongoLocalInstance.getMongoClient();
    }

    @AfterAll
    public static void closeMongo() {
        mongoLocalInstance.close();
    }

    @Test
    public void testMongo() {
        MongoDatabase database = mongoClient.getDatabase("testDB");
        MongoCollection<Document> collection = database.getCollection("testCollection");

        collection.insertOne(new Document("key", "0"));
        collection.insertOne(new Document("key", "1"));
        collection.insertOne(new Document("key", "2"));
        collection.find().forEach(doc -> System.out.println(doc.toJson()));
    }
}
