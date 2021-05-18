package com.example.kafkaeventalarm.admin;

import org.apache.kafka.clients.admin.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.Map;

@Service
public class KafkaAdminClient {

    @Value(value = "${kafka.bootstrapAddress}")
    private String bootstrapAddress;


    public ListTopicsResult getTopics(){
        AdminClient admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress));

        return admin.listTopics();
    }

    public DescribeTopicsResult getDescribeTopic(String topic){
        AdminClient admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress));

        return admin.describeTopics(Arrays.asList(topic));
    }

    public ListConsumerGroupsResult getConsumeGroups(){
        AdminClient admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress));

        return admin.listConsumerGroups();
    }

}
