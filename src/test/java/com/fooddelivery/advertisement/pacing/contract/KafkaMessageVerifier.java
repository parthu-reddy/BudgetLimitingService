package com.fooddelivery.advertisement.pacing.contract;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.cloud.contract.verifier.converter.YamlContract;
import org.springframework.cloud.contract.verifier.messaging.MessageVerifierReceiver;
import org.springframework.cloud.contract.verifier.messaging.MessageVerifierSender;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class KafkaMessageVerifier implements MessageVerifierReceiver<Message<?>>, MessageVerifierSender<Message<?>> {

    private final Map<String, BlockingQueue<Message<?>>> queues = new ConcurrentHashMap<>();

    private static final com.fasterxml.jackson.databind.ObjectMapper OBJECT_MAPPER =
            new com.fasterxml.jackson.databind.ObjectMapper();

    /**
     * Contracts declare object bodies, so the payload must reach the assertions as a Map.
     * A raw JSON String would be re-encoded into a JSON string literal, leaving JsonPath
     * with a primitive root and failing every field matcher.
     */
    private static Object parsePayload(String raw) {
        try {
            return OBJECT_MAPPER.readValue(raw, Map.class);
        } catch (Exception e) {
            return raw;
        }
    }

    @KafkaListener(id = "contract-test-listener", topics = {"ad-events"})
    public void listen(ConsumerRecord<String, String> record) {
        Map<String, Object> headers = new HashMap<>();
        record.headers().forEach(h -> headers.put(h.key(), new String(h.value())));
        Message<Object> message = MessageBuilder.createMessage(parsePayload(record.value()), new MessageHeaders(headers));
        queues.computeIfAbsent(record.topic(), k -> new LinkedBlockingQueue<>()).add(message);
    }

    @Override
    public Message<?> receive(String destination, long timeout, TimeUnit timeUnit, YamlContract contract) {
        try {
            return queues.computeIfAbsent(destination, k -> new LinkedBlockingQueue<>()).poll(timeout, timeUnit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    @Override
    public Message<?> receive(String destination, YamlContract contract) {
        return receive(destination, 5, TimeUnit.SECONDS, contract);
    }

    @Override
    public void send(Message<?> message, String destination, YamlContract contract) {
        throw new UnsupportedOperationException("Not implemented for producer tests");
    }

    @Override
    public <T> void send(T payload, Map<String, Object> headers, String destination, YamlContract contract) {
        throw new UnsupportedOperationException("Not implemented for producer tests");
    }
}
