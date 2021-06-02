package com.example.kafkaeventalarm.stream;

import com.example.kafkaeventalarm.model.Order;
import com.example.kafkaeventalarm.stream.serdes.SerdeFactory;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

@Service
public class KafkaStreamOrderProcessorWindow {
    private static final Logger logger = LoggerFactory.getLogger(KafkaStreamOrderProcessorWindow.class);

    @Value("${order.window.topic.name}")
    private String inputTopicWindow;

    @Value("${order.stream.window.output.name}")
    private String orderStreamWindowOutput;

    private KafkaStreams streams;

    @Qualifier("OrderStreamProcessorWindow")
    @Autowired
    StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    @Autowired
    public void process(@Qualifier("OrderStreamProcessorWindow") StreamsBuilder builder) {

        Map<String, Object> serdeProps = new HashMap<>();
        Serde<Order> orderSerde = SerdeFactory.createSerde(Order.class, serdeProps);
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        // Construct a `KStream` from the input topic where message values
        KStream<String, Order> textLines = builder.stream(inputTopicWindow, Consumed.with(stringSerde, orderSerde));

        // avec un WINDOW
        textLines
                .filter((key, value) -> {System.out.println("KafkaStreamOrderProcessorWindow Key=" + key + "  value=" + value); return true;})
                .selectKey((key, value) -> value.getStatus())
                .groupBy((s, order) -> order.getStatus(), Grouped.with(stringSerde, orderSerde))
                .windowedBy(TimeWindows.of(Duration.ofMinutes(1L)))
                .count(Materialized.as("countsWindow"))
                .toStream()
                .map((Windowed<String> key, Long count) -> new KeyValue<>(key.key(), count))
                .to(orderStreamWindowOutput, Produced.with(stringSerde, longSerde));

        streams = new KafkaStreams(builder.build(), streamsBuilderFactoryBean.getStreamsConfiguration());
        // Clean local store between runs
        streams.cleanUp();
        streams.start();
    }

    public ReadOnlyWindowStore<String, Order> getInteractiveQueryCountLastMinute() throws Exception {
        return streams.store("countsWindow", QueryableStoreTypes.windowStore());
    }

    public ReadOnlyKeyValueStore<String, Long> getInteractiveQueryCountWindowOutput() throws Exception {
        return streams.store(orderStreamWindowOutput, QueryableStoreTypes.keyValueStore());
    }

    @PreDestroy
    public void destroy() {
        streams.close();
    }
}