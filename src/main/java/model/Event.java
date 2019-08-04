package model;

public class Event {

    String id;
    String data;
    String status;
    boolean complete;

    public Event(String id, String data, String status, boolean complete) {
        //System.out.println("Inside Event constructor");
        this.id = id;
        this.data = data;
        this.status = status;
        this.complete = complete;
    }

    public String getId() {
        return id;
    }

    public String getData() {
        return data;
    }

    public String getStatus() {
        return status;
    }

    public boolean isComplete() { return complete; }

    public void setId(String id) {
        this.id = id;
    }

    public void setData(String data) {
        this.data = data;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public void setComplete(boolean complete) { this.complete = complete; }

    @Override
    public String toString() {
        return "Event{" +
                "id='" + id + '\'' +
                ", data='" + data + '\'' +
                ", status='" + status + '\'' +
                ", complete='" + complete + '\'' +
                '}';
    }
}
