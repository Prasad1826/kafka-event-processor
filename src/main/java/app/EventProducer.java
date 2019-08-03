package app;

import model.Event;
import model.Events;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import serde.EventSerde;
import serde.EventsSerde;

import java.util.Properties;

/**
 * @author Prasad Bonuboina
 */
public class EventProducer {
    static Properties props = new Properties();

    public static void main(String []args) {
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", EventSerde.class.getName());
        //props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        for (int i = 0; i < 1000; i++) {

            produceEvent((100+i)+"-TRD-1", "START");
        }
        //produceEvents(1);
        //produceString(10);
    }

    static void produceString(int n) {
        Producer<String, String> producer = new KafkaProducer<>(props);

        for (int i = 0; i < n; i++) {
            ProducerRecord<String, String> rec = new ProducerRecord<String, String>("streams-edtest2-output",
                    "key-"+Integer.toString(i), ""+System.currentTimeMillis());
            System.out.println(rec);
            producer.send(rec);
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        producer.flush();
        producer.close();
    }
    static void produceEvent(String eventId, String status) {

        Producer<String, Event> producer = new KafkaProducer<>(props);
        producer.send(new ProducerRecord<String, Event>("streams-edtest2-input",
                eventId.substring(0, eventId.lastIndexOf("-")),
                new Event(eventId, ""+System.currentTimeMillis(), status,false))
        );
        producer.flush();
        producer.close();
    }

    static void produceEvents(int n) {
        Producer<String, Events> producer = new KafkaProducer<>(props);

        for (int i = 0; i < n; i++) {
            ProducerRecord<String, Events> rec = new ProducerRecord<String, Events>("streams-edtest2-output",
                    "key-"+Integer.toString(i),
                    new Events(Integer.toString(i), new Event(Integer.toString(i), "Test", "START", false)));
            System.out.println(rec);
            producer.send(rec);
            System.out.println("Sent Events object");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        producer.flush();
        producer.close();
    }
}
