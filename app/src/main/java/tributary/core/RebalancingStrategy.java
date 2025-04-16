package tributary.core;

import java.util.List;

public interface RebalancingStrategy {
    public void rebalance(List<Consumer> consumers, List<Partition> partitions);

    public String getName();
}
