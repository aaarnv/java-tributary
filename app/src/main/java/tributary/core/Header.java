package tributary.core;

import java.time.LocalDate;

public class Header {
    private LocalDate date;
    private String id;
    private String payload;

    public Header(String id, String payload) {
        this.date = LocalDate.now();
        this.id = id;
        this.payload = payload;
    }

    public void print() {
        System.out.println("\tDate: " + date);
        System.out.println("\tEvent ID: " + id);
        System.out.println("\tPayload: " + payload);
    }

    public String getPayload() {
        return payload;
    }

    public String getId() {
        return id;
    }
}
