package com.ogawalucas.ecommercekafka;

import lombok.AllArgsConstructor;

import java.math.BigDecimal;

@AllArgsConstructor
public class Order {

    private final String userId, orderId;
    private final BigDecimal value;
}