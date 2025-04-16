package tributary.core;

public class Producer<T> {
    private String id;
    private MessageAllocation allocation;
    private String type;

    public Producer(String id, MessageAllocation allocation, String type) {
        this.id = id;
        this.allocation = allocation;
        this.type = type;
        System.out.println("Producer " + id + " created with type " + type + " and allocation " + allocation.getName());
    }

    public synchronized void produceEvent(Topic<?> topic, Event<?> event) {
        if (event.getType().equals(type) && topic.getType().equals(type)) {
            allocation.allocateMessage(topic, event);
        } else {
            System.err.println("Event and producer/topic types are different");
        }
    }

    public String getId() {
        return id;
    }
}
