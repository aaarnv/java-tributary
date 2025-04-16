package tributary.core;

public class ManualMessage implements MessageAllocation {
    @Override
    public synchronized void allocateMessage(Topic<?> topic, Event<?> event) {
        String partitionId = event.getKey();
        Partition p = topic.getPartition(partitionId);
        if (p == null) {
            System.err.println(
                    "Partition with id " + partitionId + " does not exist, change event key or create this partition");
            return;
        }

        p.addEvent(event);
        System.out.println("Event " + event.getId() + " added to " + partitionId);
    }

    @Override
    public String getName() {
        return "Manual";
    }
}
