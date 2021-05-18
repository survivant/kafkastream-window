package com.example.kafkaeventalarm.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
@AllArgsConstructor
public class Return {
    // KEY : UUID.toString()
    String returnId;
    String orderId;
    String returnTimestamp;

    public Return() {
    }
}
