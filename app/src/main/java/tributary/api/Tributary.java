package tributary.api;

import java.util.ArrayList;
import java.util.List;
import tributary.core.Consumer;

import tributary.core.*;

/**
 * The {@code Tributary} class manages topics, producers, and consumer groups within a messaging system.
 */
public class Tributary {
    private List<Topic<?>> topics = new ArrayList<>();
    private List<Producer<?>> producers = new ArrayList<>();
    private List<ConsumerGroup> consumerGroups = new ArrayList<>();

    /**
    * Retrieves a topic by its ID.
    *
    * @param topicId The ID of the topic to retrieve.
    * @return The topic with the specified ID, or null if no such topic exists.
    */
    public Topic<?> getTopic(String topicId) {
        for (Topic<?> topic : topics) {
            if (topic.getId().equals(topicId)) {
                return topic;
            }
        }
        return null;
    }

    /**
    * Retrieves a consumer group by its ID.
    *
    * @param groupId The ID of the consumer group to retrieve.
    * @return The consumer group with the specified ID, or null if no such group exists.
    */
    private ConsumerGroup getGroup(String groupId) {
        for (ConsumerGroup group : consumerGroups) {
            if (group.getId().equals(groupId)) {
                return group;
            }
        }
        return null;
    }

    /**
    * Creates a new topic with the specified ID and type.
    *
    * @param id   The ID of the new topic.
    * @param type The type of the new topic (e.g., "string" or "integer").
    */
    public void createTopic(String id, String type) {
        switch (type) {
        case "string":
            topics.add(new Topic<String>(id, type));
            break;
        case "integer":
            topics.add(new Topic<Integer>(id, type));
            break;
        default:
            System.err.println("invalid type: " + type);
            return;
        }
        return;
    }

    /**
    * Displays the content of a specific topic identified by topicId.
    *
    * @param topicId The ID of the topic whose content is to be displayed.
    */
    public void showTopic(String topicId) {
        getTopic(topicId).printContent();
    }

    /**
    * Displays the content of a specific consumer group identified by its ID.
    *
    * @param id The ID of the consumer group whose content is to be displayed.
    */
    public void showConsumerGroup(String id) {
        getGroup(id).printContent();
    }

    /**
    * Creates a new partition in the specified topic.
    *
    * @param id      The ID for the new partition.
    * @param topicId The ID of the topic where the partition is to be created.
    */
    public void createPartition(String id, String topicId) {
        Topic<?> topic = getTopic(topicId);
        if (topic == null) {
            System.err.println("Topic '" + topicId + "' does not exist");
            return;
        }
        topic.createPartition(id);
    }

    /**
     * Creates a new producer with the specified ID, type, and message allocation strategy.
     *
     * @param id          The ID of the new producer.
     * @param type        The type of messages the producer will handle.
     * @param allocation  The allocation strategy for how messages are distributed.
     */
    public void createProducer(String id, String type, MessageAllocation allocation) {
        switch (type) {
        case "string":
            producers.add(new Producer<String>(id, allocation, type));
            break;
        case "integer":
            producers.add(new Producer<Integer>(id, allocation, type));
            break;
        default:
            System.err.println("invalid type: " + type);
            return;
        }
        return;
    }

    /**
    * Creates a new consumer group with the specified ID, topic ID, and rebalancing strategy.
    *
    * @param id           The ID of the new consumer group.
    * @param topicId      The ID of the topic this group is subscribing to.
    * @param rebalancing  The rebalancing strategy for the consumer group.
    */
    public void createConsumerGroup(String id, String topicId, RebalancingStrategy rebalancing) {
        Topic<?> topic = getTopic(topicId);
        if (topic == null) {
            System.err.println("Topic '" + topicId + "' does not exist.");
            return;
        }
        consumerGroups.add(new ConsumerGroup(id, topic, rebalancing));
    }

    /**
    * Creates a new consumer in the specified consumer group.
    *
    * @param id       The ID of the new consumer.
    * @param groupId  The ID of the consumer group where the consumer will be added.
    */
    public void createConsumer(String id, String groupId) {
        ConsumerGroup group = getGroup(groupId);
        if (group == null) {
            System.err.println("Topic '" + groupId + "' does not exist.");
            return;
        }
        group.createConsumer(id);
    }

    /**
    * Processes a specified number of events from a given consumer and partition.
    *
    * @param consumerId     The ID of the consumer processing events.
    * @param partitionId    The ID of the partition from which events are consumed.
    * @param numberOfEvents The number of events to consume.
    */
    public void consumeEvents(String consumerId, String partitionId, int numberOfEvents) {
        getConsumer(consumerId).consumeEvents(partitionId, numberOfEvents);
    }

    /**
    * Deletes a consumer from a consumer group.
    *
    * @param groupId     The ID of the consumer group.
    * @param consumerId  The ID of the consumer to delete.
    */
    public void deleteConsumer(String groupId, String consumerId) {
        getGroup(groupId).deleteConsumer(consumerId);
    }

    /**
    * Allows a producer to send an event to a topic.
    *
    * @param producerId The ID of the producer sending the event.
    * @param topicId    The ID of the topic to which the event is sent.
    * @param event      The event to be sent.
    */
    public void produceEvent(String producerId, String topicId, Event<?> event) {
        Topic<?> topic = getTopic(topicId);
        if (topic == null) {
            System.err.println("Topic '" + topicId + "' does not exist");
            return;
        }
        for (Producer<?> producer : producers) {
            if (producer.getId().equals(producerId)) {
                producer.produceEvent(topic, event);
                return;
            }
        }
        System.err.println("Producer " + producerId + " does not exist");
    }

    /**
    * Updates the rebalancing strategy for a consumer group.
    *
    * @param groupId       The ID of the consumer group.
    * @param rangeStrategy The new rebalancing strategy to apply.
    */
    public void setConsumerGroupRebalancing(String groupId, RebalancingStrategy rangeStrategy) {
        getGroup(groupId).setRebalancingStrategy(rangeStrategy);
    }

    /**
    * Retrieves a consumer by ID.
    *
    * @param id The ID of the consumer to retrieve.
    * @return The consumer with the specified ID, or null if no such consumer exists.
    */
    public Consumer getConsumer(String id) {
        for (ConsumerGroup g : consumerGroups) {
            Consumer c = g.getConsumer(id);
            if (c != null) {
                return c;
            }
        }
        return null;
    }

    /**
    * Allows a consumer to replay events from a specified partition and starting from a given offset.
    *
    * @param consumerId The ID of the consumer replaying events.
    * @param partitionId The ID of the partition from which events are replayed.
    * @param offset The starting point for replaying events.
    */
    public void playback(String consumerId, String partitionId, int offset) {
        getConsumer(consumerId).replay(partitionId, offset);
    }

    /**
    * Retrieves a partition within a topic.
    *
    * @param topicId     The ID of the topic containing the partition.
    * @param partitionId The ID of the partition to retrieve.
    * @return The specified partition, or null if it does not exist.
    */
    public Partition getPartition(String topicId, String partitionId) {
        return getTopic(topicId).getPartition(partitionId);
    }
}
