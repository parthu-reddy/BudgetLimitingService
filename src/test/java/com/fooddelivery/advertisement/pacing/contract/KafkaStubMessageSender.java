package com.fooddelivery.advertisement.pacing.contract;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.cloud.contract.verifier.converter.YamlContract;
import org.springframework.cloud.contract.verifier.messaging.MessageVerifierSender;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.Message;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;

/**
 * Lets {@code stubTrigger.trigger(label)} actually deliver a contract's outputMessage to the
 * embedded broker.
 *
 * Without this, the consumer side of messaging CDC cannot work at all: the producer-side
 * KafkaMessageVerifier throws UnsupportedOperationException from send(), which is why the
 * pre-existing consumer tests wrapped every trigger in a catch-and-ignore.
 */
public class KafkaStubMessageSender implements MessageVerifierSender<Message<?>> {

    /** Spring adds these to every Message; they are not Kafka headers. */
    private static final Set<String> INTERNAL = Set.of("id", "timestamp");

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public KafkaStubMessageSender(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void send(Message<?> message, String destination, YamlContract contract) {
        send(message.getPayload(), message.getHeaders(), destination, contract);
    }

    @Override
    public <T> void send(T payload, Map<String, Object> headers, String destination, YamlContract contract) {
        try {
            String body = payload instanceof String
                    ? (String) payload
                    : objectMapper.writeValueAsString(payload);
            ProducerRecord<String, String> record = new ProducerRecord<>(destination, body);
            if (headers != null) {
                headers.forEach((k, v) -> {
                    if (v != null && !INTERNAL.contains(k)) {
                        record.headers().add(k, v.toString().getBytes(StandardCharsets.UTF_8));
                    }
                });
            }
            kafkaTemplate.send(record).get();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to publish stub message to " + destination, e);
        }
    }
}
