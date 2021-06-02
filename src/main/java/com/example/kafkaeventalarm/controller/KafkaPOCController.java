package com.example.kafkaeventalarm.controller;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.kafkaeventalarm.model.Order;
import com.example.kafkaeventalarm.producer.OrderProducer;
import com.example.kafkaeventalarm.stream.KafkaStreamOrderProcessorWindow;

@RestController
@RequestMapping(value = "/kafka")
public class KafkaPOCController {

    @Autowired
    private OrderProducer orderProducer;

    @Autowired
    private KafkaStreamOrderProcessorWindow kafkaStreamOrderProcessorWindow;

    @PostMapping(value = "/createOrder")
    public void sendMessageToKafkaTopic(@RequestBody Order order) {
        this.orderProducer.sendMessage(order);
    }

    @GetMapping(value = "/getInteractiveQueryCountLastMinute")
    public Map<Object, Object> getInteractiveQueryCountLastMinute() throws Exception {
        var tweetCountPerUser = new HashMap<>();
        var tweetCounts = kafkaStreamOrderProcessorWindow.getInteractiveQueryCountLastMinute().all();
        while (tweetCounts.hasNext()) {
            var next = tweetCounts.next();
            tweetCountPerUser.put(next.key, next.value);
        }
        tweetCounts.close();

        return tweetCountPerUser.entrySet().stream()
                //.sorted(Map.Entry.comparingByValue())
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (a, b) -> a,
                        LinkedHashMap::new));

    }

    @GetMapping(value = "/getInteractiveQueryCountWindowOutput")
    public Map<Object, Object> getInteractiveQueryCountWindowOutput() throws Exception {
        var tweetCountPerUser = new HashMap<>();
        var tweetCounts = kafkaStreamOrderProcessorWindow.getInteractiveQueryCountWindowOutput().all();
        while (tweetCounts.hasNext()) {
            var next = tweetCounts.next();
            tweetCountPerUser.put(next.key, next.value);
        }
        tweetCounts.close();

        return tweetCountPerUser.entrySet().stream()
                //.sorted(Map.Entry.comparingByValue())
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (a, b) -> a,
                        LinkedHashMap::new));

    }

}