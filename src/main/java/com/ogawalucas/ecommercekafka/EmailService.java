package com.ogawalucas.ecommercekafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

@Slf4j
public class EmailService {

    public static void main(String[] args) {
        new KafkaService(EmailService.class.getSimpleName(), "ECOMMERCE_SEND_EMAIL", new EmailService()::parse).run();
    }

    private void parse(ConsumerRecord<String, String> record) {
        log.info("====================================");
        log.info("Sending email");
        log.info("KEY:       " + record.key());
        log.info("VALUE:     " + record.value());
        log.info("PARTITION: " + record.partition());
        log.info("OFFSET:    " + record.offset());

        sleep(1000);
    }

    private static void sleep(long time) {
        try {
            Thread.sleep(time);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
