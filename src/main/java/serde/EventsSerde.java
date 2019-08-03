package serde;

import model.Events;

/**
 * @author Prasad Bonuboina
 */
public final class EventsSerde extends WrapperSerde<Events> {
    public EventsSerde() {
        super(new JsonSerializer<Events>(), new JsonDeserializer<Events>(Events.class));
    }
}