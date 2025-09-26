package org.example.Control;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoClient;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.example.Control.RabbitMQ.RabbitManager;
import org.example.Control.RabbitMQ.RetryRepository;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class GrpcControlService {
    private Server server;
    private final RabbitManager rabbitManager;
    private final RetryRepository repo;
    private final ObjectMapper mapper = new ObjectMapper();

    public GrpcControlService(RabbitManager rm, MongoClient mongoClient) {
        this.rabbitManager = rm;
        this.repo = new RetryRepository(mongoClient);
    }

    public void start(int port) throws Exception {
        server = ServerBuilder.forPort(port)
                .addService(new ControlServiceImpl(rabbitManager, repo)) // TODO: isso aq ta errado
                .build()
                .start();
        System.out.println("gRPC started on " + port);
    }

    public void stop() {
        if (server != null) server.shutdown();
    }

    static class ControlServiceImpl extends ControlServiceGrpc.ControlServiceImplBase {
        private final RabbitManager rm;
        private final RetryRepository repo;

        public ControlServiceImpl(RabbitManager rm, RetryRepository repo) {
            this.rm = rm;
            this.repo = repo;
        }

        @Override
        public void sendAction(ActionRequest req, StreamObserver<ActionResponse> responseObserver) {
            try {
                String id = UUID.randomUUID().toString();
                String routing = "notificacao.luz".equals(req.getDevice()) || "luz".equalsIgnoreCase(req.getDevice()) ? RabbitManager.ROUTING_LUZ : RabbitManager.ROUTING_AR;
                String payloadJson = req.getPayload();

                repo.create(id, req.getDevice(), payloadJson);

                byte[] body = payloadJson.getBytes(StandardCharsets.UTF_8);
                rm.getChannel().basicPublish(RabbitManager.EXCHANGE, routing, com.rabbitmq.client.MessageProperties.PERSISTENT_TEXT_PLAIN, body);

                ActionResponse res = ActionResponse.newBuilder().setAccepted(true).setId(id).build();
                responseObserver.onNext(res);
                responseObserver.onCompleted();

                System.out.println("Published message id=" + id + " routing=" + routing);
            } catch (Exception ex) {
                responseObserver.onError(ex);
            }
        }
    }
}
