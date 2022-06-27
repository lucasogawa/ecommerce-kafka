package com.ogawalucas.ecommercekafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map;
import java.util.regex.Pattern;

@Slf4j
public class LogService {

    public static void main(String[] args) {
        var logService = new LogService();

        try (var service = new KafkaService(
            LogService.class.getSimpleName(),
            Pattern.compile("ECOMMERCE.*"),
            logService::parse,
            String.class,
            Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
        )) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, String> record) {
        log.info("====================================");
        log.info("TOPIC:     " + record.topic());
        log.info("KEY:       " + record.key());
        log.info("VALUE:     " + record.value());
        log.info("PARTITION: " + record.partition());
        log.info("OFFSET:    " + record.offset());
    }
}
