package tributary.core;

import java.util.List;
import java.util.Random;

public class RandomMessage implements MessageAllocation {
    @Override
    public synchronized void allocateMessage(Topic<?> topic, Event<?> event) {
        if (!event.getType().equals(topic.getType())) {
            System.err.println("Event and topic types are different");
            return;
        }

        List<Partition> partitions = topic.getPartitions();
        int length = partitions.size();
        Random random = new Random();
        Partition p = partitions.get(random.nextInt(length));
        p.addEvent(event);
        System.out.println("Event " + event.getId() + " added to " + p.getId());
    }

    @Override
    public String getName() {
        return "Random";
    }
}
