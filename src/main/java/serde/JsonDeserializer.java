package serde;


import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class JsonDeserializer<T> implements Deserializer<T> {

    private Gson gson = new Gson();
    private Class<T> deserializedClass;

    public JsonDeserializer(Class<T> deserializedClass) {
        this.deserializedClass = deserializedClass;
    }

    public JsonDeserializer() {
    }

    @Override
    @SuppressWarnings("unchecked")
    public void configure(Map<String, ?> map, boolean b) {
        if(deserializedClass == null) {
            deserializedClass = (Class<T>) map.get("serializedClass");
        }
    }

    @Override
    public T deserialize(String s, byte[] bytes) {
        if(bytes == null){
            return null;
        }
        //System.out.println("s "+" "+new String(bytes));
        String str = new String(bytes);
        if(str.trim().startsWith("{"))
            return gson.fromJson(str,deserializedClass);
        else {
            System.out.println("Invalid message received inside deserialize :"+str);
            return null;
        }
    }

    @Override
    public void close() {

    }
}