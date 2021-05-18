package com.example.kafkaeventalarm.controller;

import com.example.kafkaeventalarm.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
@RequestMapping(value = "/kafka")
public class KafkaAdminController {

    @Autowired
    private KafkaAdminClient kafkaAdminClient;

    @GetMapping(value = "/getTopics")
    public Collection<String> getTopics() throws Exception {
        Collection<TopicListing> topicListings = kafkaAdminClient.getTopics().listings().get();

        return topicListings.stream().map(topicListing -> topicListing.name()).collect(Collectors.toList());
    }

    @GetMapping(value = "/getConsumerGroups")
    public Collection<String> getConsumerGroups() throws Exception {
        Collection<ConsumerGroupListing> consumerGroupListings = kafkaAdminClient.getConsumeGroups().all().get();

        return consumerGroupListings.stream().map(consumerGroupListing -> consumerGroupListing.groupId()).collect(Collectors.toList());
    }

    @GetMapping(value = "/describeTopic/{topic}")
    public Map<String, TopicDescription> describeTopic(@RequestParam String topic) throws Exception {
        DescribeTopicsResult describeTopic = kafkaAdminClient.getDescribeTopic(topic);

        Map<String, TopicDescription> map = describeTopic.all().get();
        // nothing useful for now
        return map;
    }

}