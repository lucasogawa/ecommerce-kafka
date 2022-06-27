package com.ogawalucas.ecommercekafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

@Slf4j
public class FraudDetectorService {

    public static void main(String[] args) {
        var fraudDetectorService = new FraudDetectorService();

        try (var service = new KafkaService(FraudDetectorService.class.getSimpleName(), "ECOMMERCE_NEW_ORDER", fraudDetectorService::parse)) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, String> record) {
        log.info("====================================");
        log.info("Processing new order, checking for fraud...");
        log.info("KEY:       " + record.key());
        log.info("VALUE:     " + record.value());
        log.info("PARTITION: " + record.partition());
        log.info("OFFSET:    " + record.offset());

        sleep(5000);

    }

    private static void sleep(long time) {
        try {
            Thread.sleep(time);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
