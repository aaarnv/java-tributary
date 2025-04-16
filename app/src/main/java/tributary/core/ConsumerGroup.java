package tributary.core;

import java.util.ArrayList;
import java.util.List;

public class ConsumerGroup {
    private String id;
    private Topic<?> topic;
    private List<Consumer> consumers = new ArrayList<>();
    private RebalancingStrategy strategy;

    public ConsumerGroup(String id, Topic<?> topic, RebalancingStrategy strategy) {
        this.id = id;
        this.strategy = strategy;
        this.topic = topic;
        System.out.println("Consumer Group: " + id + " with rebalancing strategy " + strategy.getName() + " in topic: "
                + topic.getId());
    }

    public void createConsumer(String cId) {
        consumers.add(new Consumer(cId, this.id));
        rebalance();
    }

    public void deleteConsumer(String id) {
        Consumer c = getConsumer(id);
        if (c == null) {
            System.err.println("consumer does not exist in group " + this.id);
            return;
        }
        consumers.remove(c);
        System.out.println("Consumer " + id + " successfully removed from " + this.id);
        rebalance();
        printContent();
        return;
    }

    public void rebalance() {
        if (consumers.size() == 0) {
            System.err.println("No consumers to rebalance");
            return;
        }
        for (Consumer c : consumers) {
            c.resetPartitions();
        }

        strategy.rebalance(consumers, topic.getPartitions());
    }

    public Consumer getConsumer(String id) {
        for (Consumer consumer : consumers) {
            if (consumer.getId().equals(id)) {
                return consumer;
            }
        }
        return null;
    }

    public void setRebalancingStrategy(RebalancingStrategy rebalancing) {
        this.strategy = rebalancing;
        System.out.println("Consumer Group " + id + " rebalancing changed to " + strategy.getName());
        rebalance();
    }

    public String getId() {
        return id;
    }

    public void printContent() {
        System.out.println("Consumer Group ID: " + id);
        System.out.println("Rebalancing Strategy: " + strategy.getName());
        System.out.println("Consumers:");

        for (Consumer consumer : consumers) {
            System.out.println("  Consumer ID: " + consumer.getId());
            System.out.println("    Partitions:");
            for (Partition partition : consumer.getPartitions()) {
                System.out.print("      " + partition.getId() + "\n");
            }
            System.out.println("    Events Consumed:");
            for (Event<?> e : consumer.getConsumedEvents()) {
                e.print();
            }
            System.out.println();
        }
    }

}
