package com.example.kafkaeventalarm.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import java.util.HashMap;
import java.util.Map;

@Configuration
//@EnableKafkaStreams
public class KafkaStreamProcessorConfig {
    private final Logger logger = LoggerFactory.getLogger(KafkaStreamProcessorConfig.class);

    @Value(value = "${kafka.bootstrapAddress}")
    private String bootstrapAddress;

    @Value("${order.topic.group.id-stream}")
    private String orderGroupId;

    // DEFAULT Kafka Stream Configuration Bean
    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public StreamsConfig kStreamsConfigs() {
        Map<String, Object> config = new HashMap<>();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "default");
        setDefaults(config);
        return new StreamsConfig(config);
    }

    // DEFAULTS
    public void setDefaults(Map<String, Object> config) {
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    @Bean("OrderStreamProcessorWindow")
    public StreamsBuilderFactoryBean orderStreamProcessorWindowStreamBuilderFactoryBean() {
        Map<String, Object> config = new HashMap<>();
        setDefaults(config);
        //config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "OrderStreamProcessorWindow");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass());
        config.put(ConsumerConfig.CLIENT_ID_CONFIG, "OrderStreamProcessorWindow");
        config.put(ConsumerConfig.GROUP_ID_CONFIG, orderGroupId+"_window");
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 5000);
        return new StreamsBuilderFactoryBean(new KafkaStreamsConfiguration(config));
    }

}