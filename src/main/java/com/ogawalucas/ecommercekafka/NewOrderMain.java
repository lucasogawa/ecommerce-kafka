package com.ogawalucas.ecommercekafka;

import lombok.extern.slf4j.Slf4j;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

@Slf4j
public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try (var dispatcher = new KafkaDispatcher()) {
            for (int i = 0; i < 10; i++) {
                var key = UUID.randomUUID().toString();
                var value = key + "123_456_7890";

                dispatcher.send("ECOMMERCE_NEW_ORDER", key, value);

                var email = "Thank you for your order! Whe are processing your order!";
                dispatcher.send("ECOMMERCE_SEND_EMAIL", key, email);
            }
        } catch (Exception ex) {

        }
    }
}
