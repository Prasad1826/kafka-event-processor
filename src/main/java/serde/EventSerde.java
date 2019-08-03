package serde;

import model.Event;

/**
 * @author Prasad Bonuboina
 */
public final class EventSerde extends WrapperSerde<Event> {
    public EventSerde() {
        super(new JsonSerializer<Event>(), new JsonDeserializer<Event>(Event.class));
    }
}