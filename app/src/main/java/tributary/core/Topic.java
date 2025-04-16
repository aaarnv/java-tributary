package tributary.core;

import java.util.ArrayList;
import java.util.List;

public class Topic<T> {
    private String id;
    private String type;
    private List<Partition> partitions = new ArrayList<>();

    public List<Partition> getPartitions() {
        return partitions;
    }

    public Topic(String id, String type) {
        this.id = id;
        this.type = type;
        System.out.println("Topic " + id + " created with type " + type);
    }

    public String getId() {
        return id;
    }

    public void createPartition(String id) {
        partitions.add(new Partition(id));
        System.out.println("Partition '" + id + "' created in Topic: " + this.id);
    }

    public Partition getPartition(String partitionId) {
        for (Partition p : partitions) {
            if (p.getId().equals(partitionId)) {
                return p;
            }
        }
        return null;
    }

    public void printContent() {
        System.out.println("Topic ID: " + id);
        System.out.println("Type: " + type);

        for (Partition partition : partitions) {
            System.out.println("Partition IDs: " + partition.getId());
            System.out.println("Events:");
            for (Event<?> event : partition.getEvents()) {
                event.print();
            }
        }
    }

    public String getType() {
        return type;
    }
}
