package model;

import java.util.HashMap;
import java.util.Map;

public class Events {

    String eventId;
    Map<String, Event> events = new HashMap<>();
    String inProcessId = "";
    String toBePublished = "";

    public Events(String eventId, Event event) {
        System.out.println("Inside Events constructor<String, Event>");
        initialize(eventId, event);
    }
    private void initialize(String eventId, Event event) {
        System.out.println(eventId+" "+event);
        if(eventId==null || eventId.isEmpty())
            this.eventId = event.getId().substring(0, event.getId().lastIndexOf("-"));
        else
            this.eventId = eventId;

        this.events.put(event.getId(), event);
        this.inProcessId = event.getId();
    }
    public Events(String eventId, Map<String, Event> events, String inProcessId, String toBePublished) {
        System.out.println("Inside Events constructor..");
        this.eventId = eventId;
        this.events = events;
        this.inProcessId = inProcessId;
        this.toBePublished = toBePublished;
    }

    public Events() {
       // System.out.println("Inside Events constructor");
        eventId = "";
    }

    public Events process(String eventId, Event event) {

        toBePublished="";
        String newStatus = event.getStatus();
        if(inProcessId.isEmpty())
            inProcessId = event.getId();

        if(events.size()==0) {
            initialize(eventId, event);
        } else if(events.containsKey(event.getId())) {
            event.setStatus(events.get(event.getId()).getStatus()+"|"+event.getStatus());
            events.put(event.getId(), event);
        } else {
            events.put(event.getId(), event);
        }
        System.out.println("Inside Events process "+event+" size is "+events.size());
        if(events.get(inProcessId).getId() == event.getId()) {
            if(newStatus.equalsIgnoreCase("stop")){
                //event reached logic end. remove this and publish next one if exists
                System.out.println("EventId "+event.getId()+" reached logical end.");
                events.remove(inProcessId);
                if(events.size() > 0) {
                    Map.Entry<String, Event> eventToPublish = events.entrySet().iterator().next();
                    inProcessId = eventToPublish.getKey();
                    toBePublished=inProcessId;
                } else
                    System.out.println("No more events left for Event key "+eventId);
            } else {
                //publish this event
                toBePublished=inProcessId;
                //getEventToBePublished();
            }
        }
        if(toBePublished.isEmpty())
            System.out.println("Not publishing any event as there is another event "+inProcessId+" already in process. So toBePublished : "+toBePublished);
        else
            System.out.println("Event to be published is "+toBePublished);
        return this;
    }

    public Event remove(String id) {
        System.out.println("Inside Events remove " +id);
        return events.remove(id);
    }

    @Override
    public String toString() {
        return "Events{" +
                "eventId='" + eventId + '\'' +
                ", events=" + events +
                ", inProcessId=" + inProcessId +
                '}';
    }
    public boolean hasTobeProcessed() {
        if(toBePublished.isEmpty())
            return false;
        return true;
    }

    public Event getToBeProcessed() {
        return events.get(toBePublished);
    }
}
