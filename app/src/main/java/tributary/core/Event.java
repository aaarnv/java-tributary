package tributary.core;

public class Event<T> {
    private Header headers;
    private String keyId;
    private T value;

    public Event(String id, String payload, String key, T value) {
        this.headers = new Header(id, payload);
        this.keyId = key;
        this.value = value;
    }

    public synchronized void print() {
        System.out.println("{");
        System.out.println("\tEvent Details:");
        headers.print();
        System.out.println("\tKey ID: " + keyId);
        System.out.println("\tValue: " + value);
        System.out.println("}");
    }

    public String getType() {
        return headers.getPayload();
    }

    public String getKey() {
        return keyId;
    }

    public String getId() {
        return headers.getId();
    }
}
