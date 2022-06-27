package com.ogawalucas.serviceemail;

import com.ogawalucas.commonkafka.KafkaService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;

@Slf4j
public class EmailService {

    public static void main(String[] args) {
        var emailService = new EmailService();

        try (var service = new KafkaService(
            EmailService.class.getSimpleName(),
            "ECOMMERCE_SEND_EMAIL",
            emailService::parse,
            String.class,
            Map.of()
        )) {
            service.run();
        }
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
