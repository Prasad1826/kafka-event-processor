package serde;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;


public class WrapperSerde<T> implements Serde<T>, Serializer<T>, Deserializer<T> {

    final private Serializer<T> serializer;
    final private Deserializer<T> deserializer;

    public WrapperSerde(Serializer<T> serializer, Deserializer<T> deserializer) {
        this.serializer = serializer;
        this.deserializer = deserializer;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        serializer.configure(configs, isKey);
        deserializer.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String s, T t) {
        //System.out.println("Inside default serialize");
        //return new byte[0];
        return serializer.serialize(s, t);
    }

    @Override
    public T deserialize(String s, byte[] bytes) {
        //System.out.println("Inside default deserialize");
        return deserializer.deserialize(s, bytes);
        //return null;
    }

    @Override
    public void close() {
        serializer.close();
        deserializer.close();
    }

    @Override
    public Serializer<T> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<T> deserializer() {
        return deserializer;
    }
}
