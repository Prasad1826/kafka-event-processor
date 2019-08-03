package app;

import model.Event;
import model.Events;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import serde.EventSerde;
import serde.EventsSerde;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * @author Prasad Bonuboina
 */
public class EventConsumer {
    static Properties props = new Properties();

    public static void main(String []args) {
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "edtest2");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", EventSerde.class.getName());
        //props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        consumeEvent();
        //consumeString();
    }

    static void consumeEvent() {
        System.out.println("Inside consumeEvents");
        KafkaConsumer<String, Event> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("streams-edtest2-output"));

        while (true) {
            ConsumerRecords<String, Event> records = consumer.poll(Duration.ofMillis(10));
            for (ConsumerRecord<String, Event> record : records) {
                Event event = record.value();
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value()+" "+(System.currentTimeMillis()-Long.parseLong(event.getData())));
                //System.out.println("Finished checking/printing messages");
            }
        }
    }

    static void consumeString() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("streams-edtest2-output"));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                //System.out.println("Finished checking/printing messages");
            }
        }
    }
}
