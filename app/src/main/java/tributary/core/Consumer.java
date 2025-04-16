package tributary.core;

import java.util.ArrayList;
import java.util.List;

public class Consumer {
    private String id;
    private String groupId;
    private List<Partition> partitions;
    private List<Event<?>> consumedEvents = new ArrayList<>();

    public Consumer(String id, String groupId) {
        this.id = id;
        this.groupId = groupId;
        this.partitions = new ArrayList<>();
        System.out.println("Consumer " + id + " created in " + groupId);
    }

    public synchronized void consumeEvents(String partitionId, int numberOfEvents) {
        Partition p = getPartition(partitionId);
        if (p == null) {
            System.out.println("partitionId does not exist for this consumer");
            return;
        }

        for (int i = 0; i < numberOfEvents; i++) {
            Event<?> e = p.consumeEvent(groupId);
            if (e != null)
                consumedEvents.add(e);
        }
    }

    public void replay(String partitionId, int offset) {
        Partition p = getPartition(partitionId);
        if (p == null) {
            System.out.println("Invalid partitionId");
            return;
        }

        int currentIndex = p.getCurrentIndex(groupId);
        p.setConsumptionIndex(groupId, offset);
        if (offset < currentIndex) {
            System.out.println("Replaying from offset " + offset);
            while (offset < currentIndex && currentIndex < p.getSize()) {
                Event<?> e = p.consumeEvent(groupId);
                if (e != null)
                    consumedEvents.add(e);
                offset++;
            }
        } else {
            System.err.println("Replay offset is greater than the current index.");
        }
    }

    public String getId() {
        return id;
    }

    public Partition getPartition(String partitionId) {
        for (Partition p : partitions) {
            if (p.getId().equals(partitionId)) {
                return p;
            }
        }
        return null;
    }

    public void addPartition(Partition partition) {
        partitions.add(partition);
    }

    public List<Partition> getPartitions() {
        return partitions;
    }

    public void resetPartitions() {
        partitions = new ArrayList<>();
    }

    public List<Event<?>> getConsumedEvents() {
        return consumedEvents;
    }
}
