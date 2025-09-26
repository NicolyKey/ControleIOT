package org.example.Control.RabbitMQ;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class RabbitManager {
    public static final String EXCHANGE = "Exchange";
    public static final String DLX = "DLX_Exchange";
    public static final String DELAY_QUEUE = "Delay_10s";
    public static final String QUEUE_LUZ = "Fila_AcionamentosLuz";
    public static final String QUEUE_AR = "Fila_AcionamentosAr";
    public static final String ROUTING_LUZ = "notificacao.luz";
    public static final String ROUTING_AR = "notificacao.ar";

    private final Connection conn;
    private final Channel ch;

    public RabbitManager(String rabbitUri) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri(rabbitUri);
        this.conn = factory.newConnection();
        this.ch = conn.createChannel();
    }

    public void declareInfrastructure() throws Exception {
        ch.exchangeDeclare(EXCHANGE, "topic", true);
        ch.exchangeDeclare(DLX, "fanout", true);

        java.util.Map<String, Object> delayArgs = new java.util.HashMap<>();
        delayArgs.put("x-message-ttl", 10000); // 10s
        delayArgs.put("x-dead-letter-exchange", EXCHANGE);
        ch.queueDeclare(DELAY_QUEUE, true, false, false, delayArgs);
        ch.queueBind(DELAY_QUEUE, DLX, "");

        java.util.Map<String, Object> qargs = new java.util.HashMap<>();
        qargs.put("x-dead-letter-exchange", DLX);
        ch.queueDeclare(QUEUE_LUZ, true, false, false, qargs);
        ch.queueBind(QUEUE_LUZ, EXCHANGE, ROUTING_LUZ);

        ch.queueDeclare(QUEUE_AR, true, false, false, qargs);
        ch.queueBind(QUEUE_AR, EXCHANGE, ROUTING_AR);

        System.out.println("Rabbit infra declared.");
    }

    public Channel getChannel() {
        return ch;
    }

    public void close() throws Exception {
        ch.close();
        conn.close();
    }
}
