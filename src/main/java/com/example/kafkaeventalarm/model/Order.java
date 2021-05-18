package com.example.kafkaeventalarm.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
@AllArgsConstructor
public class Order {
    // KEY : UUID.toString()
    String orderId;
    String product;
    String orderTimestamp;

    // CREATED, SENT, RECEIVED, RETURNED
    String status;

    public Order() {
    }
}
