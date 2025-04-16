package tributary.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Partition {
    private String id;
    private List<Event<?>> events = new ArrayList<>();
    private Map<String, Integer> consumptionIndexes = new HashMap<>();

    public Partition(String id) {
        this.id = id;
    }

    public synchronized Event<?> consumeEvent(String consumerGroupId) {
        Integer currentIndex = consumptionIndexes.get(consumerGroupId);
        if (currentIndex == null) {
            currentIndex = 0;
        }

        if (currentIndex >= events.size()) {
            System.err.println("Error: No events left to be consumed");
            return null;
        }

        Event<?> event = events.get(currentIndex);
        currentIndex++;
        consumptionIndexes.put(consumerGroupId, currentIndex);
        event.print();
        return event;
    }

    public String getId() {
        return id;
    }

    public void addEvent(Event<?> event) {
        events.add(event);
    }

    public int getSize() {
        return events.size();
    }

    public List<Event<?>> getEvents() {
        return events;
    }

    public void setConsumptionIndex(String groupId, int index) {
        consumptionIndexes.put(groupId, index);
    }

    public int getCurrentIndex(String consumerGroupId) {
        return consumptionIndexes.getOrDefault(consumerGroupId, 0);
    }
}
