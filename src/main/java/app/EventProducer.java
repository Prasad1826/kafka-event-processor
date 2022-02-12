package app;

import model.Event;
import model.EventStore;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import serde.EventSerde;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author Prasad Bonuboina
 */
public class EventProducer {
    static Properties props = new Properties();

    public static void main(String []args) throws ExecutionException, InterruptedException {
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("batch.size", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", EventSerde.class.getName());
        //props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        produceEvent();
        //produceEvents(1);
        //produceString(10);
    }

    static void produceString(int n) {
        Producer<String, String> producer = new KafkaProducer<>(props);

        for (int i = 0; i < n; i++) {
            ProducerRecord<String, String> rec = new ProducerRecord<String, String>("streams-ep1-output",
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
    static void produceEvent() throws ExecutionException, InterruptedException {

        Producer<String, Event> producer = new KafkaProducer<>(props);
        int count = 1;
        String status = "START";
        for (int i = 0; i < count ; i++) {
            String eventId = (1000+i)+"-1";
            String key = eventId.substring(0, eventId.lastIndexOf("-"));
            //System.out.println(key);
            Future<RecordMetadata> future = producer.send(new ProducerRecord<>("streams-ep1-input",
                    key,
                    new Event(eventId, "" + System.currentTimeMillis(), status, false))
            );
            producer.flush();
            System.out.println("eventId: "+ eventId + " Offset : " + future.get().offset());
        }
        status = "START";
        for (int i = 0; i < count ; i++) {
            String eventId = (1000+i)+"-2";
            String key = eventId.substring(0, eventId.lastIndexOf("-"));
            //System.out.println(key);
            Future<RecordMetadata> future = producer.send(new ProducerRecord<>("streams-ep1-input",
                    key,
                    new Event(eventId, "" + System.currentTimeMillis(), status, false))
            );
            producer.flush();
            System.out.println("eventId: "+ eventId + " Offset : " + future.get().offset());
        }
        status = "STOP";
        for (int i = 0; i < count ; i++) {
            String eventId = (1000+i)+"-1";
            String key = eventId.substring(0, eventId.lastIndexOf("-"));
            //System.out.println(key);
            Future<RecordMetadata> future = producer.send(new ProducerRecord<>("streams-ep1-input",
                    key,
                    new Event(eventId, "" + System.currentTimeMillis(), status, false))
            );
            producer.flush();
            System.out.println("eventId: "+ eventId + " Offset : " + future.get().offset());
        }
        status = "STOP";
        for (int i = 0; i < count ; i++) {
            String eventId = (1000+i)+"-2";
            String key = eventId.substring(0, eventId.lastIndexOf("-"));
            //System.out.println(key);
            Future<RecordMetadata> future = producer.send(new ProducerRecord<>("streams-ep1-input",
                    key,
                    new Event(eventId, "" + System.currentTimeMillis(), status, false))
            );
            producer.flush();
            System.out.println("eventId: "+ eventId + " Offset : " + future.get().offset());
        }
        producer.close();
    }

    static void produceEvents(int n) {
        Producer<String, EventStore> producer = new KafkaProducer<>(props);

        for (int i = 0; i < n; i++) {
            ProducerRecord<String, EventStore> rec = new ProducerRecord<String, EventStore>("streams-ep1-output",
                    "key-"+Integer.toString(i),
                    new EventStore(Integer.toString(i), new Event(Integer.toString(i), "Test", "START", false)));
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
