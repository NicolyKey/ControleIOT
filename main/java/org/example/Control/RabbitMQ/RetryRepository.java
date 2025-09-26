package org.example.Control.RabbitMQ;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

public class RetryRepository {
    private final MongoCollection<Document> coll;

    public RetryRepository(MongoClient client) {
        this.coll = client.getDatabase("control_db").getCollection("retries");
    }

    public void create(String id, String device, String payloadJson) {
        Document d = new Document("_id", id)
                .append("device", device)
                .append("payload", Document.parse(payloadJson))
                .append("attempts", 0)
                .append("status", "pending")
                .append("createdAt", java.time.Instant.now().toString());
        coll.insertOne(d);
    }

    public void incrementAttempt(String id, String lastError) {
        coll.updateOne(new Document("_id", id),
                new Document("$inc", new Document("attempts", 1))
                        .append("$set", new Document("lastError", lastError).append("updatedAt", java.time.Instant.now().toString())));
    }

    public void markProcessed(String id) {
        coll.updateOne(new Document("_id", id),
                new Document("$set", new Document("status", "processed").append("updatedAt", java.time.Instant.now().toString())));
    }

    public void markFailed(String id) {
        coll.updateOne(new Document("_id", id),
                new Document("$set", new Document("status", "failed").append("updatedAt", java.time.Instant.now().toString())));
    }

    // find pending with attempts >= limit
    public Iterable<Document> findPendingWithAttemptsGE(int limit) {
        return coll.find(new Document("status", "pending").append("attempts", new Document("$gte", limit)));
    }
}

