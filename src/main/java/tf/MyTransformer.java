package tf;


import model.Event;
import model.EventStore;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import serde.EventSerde;

import java.time.Duration;

public class MyTransformer implements Transformer<String, EventStore,KeyValue<String, Event>> {
    private static final Logger logger = LogManager.getLogger(MyTransformer.class);

    private final String storeName;
    private final String topic;
    private ProcessorContext context;
    private KeyValueStore<String, EventSerde> stateStore;

    public MyTransformer(String storeName, String topic) {
        super();
        this.storeName = storeName;
        this.topic = topic;
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.stateStore = context.getStateStore(storeName);
        // punctuate each second; can access this.state
        context.schedule(Duration.ofSeconds(30), PunctuationType.WALL_CLOCK_TIME, new Punctuator() {

            @Override
            public void punctuate(long l) {
                int size = 0;
                try (final KeyValueIterator<String, EventSerde> all = stateStore.all()) {
                    while (all.hasNext()) {
                        final KeyValue<String, EventSerde> record = all.next();
                        logger.info(record.key);
                        size++;
                    }
                }
                logger.info("Total messages found " + size);
            }
        });
    }

    @Override
    public KeyValue<String, Event> transform(String s, EventStore s2) {
        logger.info("Inside Transform for "+ s +" " + s2);
        return null;
    }

    @Override
    public void close() {
        logger.warn("inside close");
    }
}
