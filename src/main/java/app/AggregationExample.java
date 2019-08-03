package app;

import model.Event;
import model.Events;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import serde.*;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public final class AggregationExample {

    public static void main(final String[] args) {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-eventprocessor2");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        //props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, new EventSerde().getClass().getName());

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        // Note: To re-run the demo, you need to use the offset reset tool:
        // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, Event> source = builder.stream("streams-edtest2-input");

        final KTable<String, Events> eventDispatcher = source.groupByKey()
                .aggregate((Initializer<Events>) () -> new Events(),
                        (Aggregator<String, Event, Events>) (key, event, events) -> events.process(key, event),
                        Materialized.<String, Events, KeyValueStore<Bytes, byte[]>>as("test-view-002")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new EventsSerde())
                );
        eventDispatcher.toStream()
                .filter((key, value)->value.hasTobeProcessed())
                .map((key, value)-> KeyValue.pair(key,value.getToBeProcessed()))
                .to("streams-edtest2-output", Produced.with(Serdes.String(), new EventSerde()));

        Topology topology = builder.build();

        KafkaStreams streams = new KafkaStreams(topology, props);

        System.out.println(topology.describe());

        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-wordcount-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}