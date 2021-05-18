package com.example.kafkaeventalarm.producer;

import com.example.kafkaeventalarm.model.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class OrderProducer {
    private static final Logger logger = LoggerFactory.getLogger(OrderProducer.class);

    @Value("${order.window.topic.name}")
    private String inputTopicWindow;

    @Autowired
    private KafkaTemplate<String, Order> kafkaTemplate;

    public void sendMessage(Order order) {
        logger.debug(String.format("#### -> Producing message -> %s", order));
        this.kafkaTemplate.send(inputTopicWindow, order.getOrderId(), order);
    }
}
