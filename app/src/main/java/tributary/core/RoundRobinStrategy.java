package tributary.core;

import java.util.List;

public class RoundRobinStrategy implements RebalancingStrategy {
    @Override
    public void rebalance(List<Consumer> consumers, List<Partition> partitions) {
        int numConsumers = consumers.size();
        int numPartitions = partitions.size();

        // Index to keep track of the next partition to allocate
        int partitionIndex = 0;

        for (int i = 0; i < numPartitions; i++) {
            Partition partition = partitions.get(i);
            Consumer consumer = consumers.get(partitionIndex % numConsumers);

            consumer.addPartition(partition);

            partitionIndex++;
        }
    }

    @Override
    public String getName() {
        return "RoundRobin";
    }
}
