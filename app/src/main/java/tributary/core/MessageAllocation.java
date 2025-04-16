package tributary.core;

public interface MessageAllocation {
    public void allocateMessage(Topic<?> topic, Event<?> event);

    public String getName();
}
