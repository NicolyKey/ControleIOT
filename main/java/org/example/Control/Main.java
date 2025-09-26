package org.example.Control;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.example.Control.RabbitMQ.RabbitManager;

public class Main {
    public static void main(String[] args) throws Exception {
        String rabbitUrl = System.getenv().getOrDefault("RABBITMQ_URL", "amqp://guest:guest@localhost:5672");
        String mongoUrl = System.getenv().getOrDefault("MONGO_URL", "mongodb://localhost:27017");
        int grpcPort = Integer.parseInt(System.getenv().getOrDefault("GRPC_PORT", "50051"));

        MongoClient mongoClient = MongoClients.create(mongoUrl);

        RabbitManager rm = new RabbitManager(rabbitUrl);
        rm.declareInfrastructure();

        GrpcControlService grpcService = new GrpcControlService(rm, mongoClient);
        grpcService.start(grpcPort);

        ConsumerSimulator sim = new ConsumerSimulator(rm, mongoClient);
        sim.startConsuming();

        BackgroundWorker worker = new BackgroundWorker(mongoClient);
        worker.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                grpcService.stop();
                rm.close();
                mongoClient.close();
                worker.stop();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }));
    }
}
