package tributary.core;

import java.util.List;

public class RangeStrategy implements RebalancingStrategy {
    @Override
    public void rebalance(List<Consumer> consumers, List<Partition> partitions) {
        int numConsumers = consumers.size();
        int numPartitions = partitions.size();

        int partitionsPerConsumer = numPartitions / numConsumers;
        int partitionsForFirstConsumer = partitionsPerConsumer + (numPartitions % numConsumers);

        int partitionIndex = 0;

        for (int i = 0; i < numConsumers; i++) {
            int partitionsToAllocate = (i == 0) ? partitionsForFirstConsumer : partitionsPerConsumer;
            Consumer consumer = consumers.get(i);
            for (int j = 0; j < partitionsToAllocate; j++) {
                Partition partition = partitions.get(partitionIndex);
                consumer.addPartition(partition);
                partitionIndex++;
            }
        }
    }

    @Override
    public String getName() {
        return "Range";
    }
}
