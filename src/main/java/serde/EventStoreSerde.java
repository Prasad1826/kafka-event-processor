package serde;

import model.EventStore;

/**
 * @author Prasad Bonuboina
 */
public final class EventStoreSerde extends WrapperSerde<EventStore> {
    public EventStoreSerde() {
        super(new JsonSerializer<EventStore>(), new JsonDeserializer<EventStore>(EventStore.class));
    }
}