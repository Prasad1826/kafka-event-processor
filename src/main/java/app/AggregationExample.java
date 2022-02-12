package app;

import model.Event;
import model.EventStore;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import serde.EventSerde;
import serde.EventStoreSerde;
import tf.MyTransformer;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public final class AggregationExample {
    private static final Logger logger = LogManager.getLogger(AggregationExample.class);
    public static final String INPUT_TOPIC = "streams-ep1-input";
    public static final String STORE_NAME = "event-processor";

    public static void main(final String[] args) {
        final Properties props = new Properties();
        //props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-ep1");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/Users/prasadbonuboina/kstream");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, EventSerde.class.getName());

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, Event> source = builder.stream(INPUT_TOPIC);

        final KTable<String, EventStore> eventDispatcher = source.groupByKey()
                .aggregate(EventStore::new,
                        (key, event, events) -> events.process(key, event),
                        Materialized.<String, EventStore, KeyValueStore<Bytes, byte[]>>as(STORE_NAME)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new EventStoreSerde())
                );
        eventDispatcher.toStream()
                .filter((key, value)->value.hasTobeProcessed())
                .map((key, value)-> KeyValue.pair(key,value.getToBeProcessed()))
                .to("streams-ep1-output", Produced.with(Serdes.String(), new EventSerde()));


        eventDispatcher.toStream().transform((TransformerSupplier) () -> new MyTransformer(AggregationExample.STORE_NAME, AggregationExample.INPUT_TOPIC), STORE_NAME);
        //createTTL(builder, eventDispatcher);

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
            streams.setUncaughtExceptionHandler(throwable -> {
                System.out.println("Uncaught exception");
                logger.error("", throwable);
                return null;
            });
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}