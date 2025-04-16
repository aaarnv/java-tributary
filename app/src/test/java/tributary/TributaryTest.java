package tributary;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;

import tributary.api.Tributary;
import tributary.core.Consumer;
import tributary.core.Event;
import tributary.core.ManualMessage;
import tributary.core.Partition;
import tributary.core.RandomMessage;
import tributary.core.RangeStrategy;
import tributary.core.RoundRobinStrategy;

public class TributaryTest {
    private final PrintStream originalOut = System.out;
    private final PrintStream originalErr = System.err;
    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();

    @BeforeEach
    public void setUpStreams() {
        System.setOut(new PrintStream(outContent));
        System.setErr(new PrintStream(errContent));
    }

    @AfterEach
    public void restoreStreams() {
        System.setOut(originalOut);
        System.setErr(originalErr);
    }

    @Test
    public void testSuccessfulTopicCreation() {
        Tributary tributary = new Tributary();

        tributary.createTopic("t1", "string");
        assertTrue(tributary.getTopic("t1").getId().equals("t1"));
        assertEquals(tributary.getTopic("t1").getType(), "string");

        tributary.createTopic("t2", "integer");
        assertTrue(tributary.getTopic("t2").getId().equals("t2"));
        assertEquals(tributary.getTopic("t2").getType(), "integer");
    }

    @Test
    public void testSuccessfulPartitionCreation() {
        Tributary tributary = new Tributary();

        tributary.createTopic("t1", "string");
        tributary.createPartition("p1", "t1");

        String actualOutput = outContent.toString().trim();
        assertTrue(actualOutput.contains("Partition 'p1' created in Topic: t1"));

        tributary.showTopic("t1");
        actualOutput = outContent.toString().trim();
        assertTrue(actualOutput.contains("Partition IDs: p1"));
    }

    @Test
    public void testSuccessfulProducerCreation() {
        Tributary tributary = new Tributary();

        tributary.createProducer("prod1", "string", new RandomMessage());

        String actualOutput = outContent.toString().trim();
        assertTrue(actualOutput.contains("Producer prod1 created with type string and allocation Random"));
    }

    @Test
    public void testSuccessfulConsumerGroupCreation() {
        Tributary tributary = new Tributary();

        tributary.createTopic("t1", "string");
        tributary.createConsumerGroup("g1", "t1", new RangeStrategy());
        tributary.createConsumerGroup("g2", "t1", new RoundRobinStrategy());
        String actualOutput = outContent.toString().trim();
        assertTrue(actualOutput.contains("Consumer Group: g1 with rebalancing strategy Range in topic: t1"));
        assertTrue(actualOutput.contains("Consumer Group: g2 with rebalancing strategy RoundRobin in topic: t1"));
    }

    @Test
    public void testSuccessfulConsumerCreation() {
        Tributary tributary = new Tributary();

        tributary.createTopic("t1", "string");
        tributary.createConsumerGroup("g1", "t1", new RangeStrategy());
        tributary.createConsumer("c1", "g1");
        String actualOutput = outContent.toString().trim();
        assertTrue(actualOutput.contains("Consumer c1 created in g1"));
    }

    @Test
    public void testRandomProduce() {
        Tributary tributary = new Tributary();

        tributary.createTopic("t1", "string");
        tributary.createPartition("p1", "t1");
        tributary.createPartition("p2", "t1");
        tributary.createPartition("p3", "t1");
        tributary.createProducer("prod1", "string", new RandomMessage());

        Event<String> e = new Event<String>("event1", "string", "p1", "valueee");
        tributary.produceEvent("prod1", "t1", e);
        tributary.showTopic("t1");
        String actualOutput = outContent.toString().trim();
        assertTrue(actualOutput.contains("event1"));
    }

    @Test
    public void testManualProduce() {
        Tributary tributary = new Tributary();

        tributary.createTopic("t1", "string");
        tributary.createPartition("p1", "t1");
        tributary.createPartition("p2", "t1");
        tributary.createPartition("p3", "t1");
        tributary.createProducer("prod1", "string", new ManualMessage());
        Event<String> e = new Event<String>("event1", "string", "p2", "valueee");
        tributary.produceEvent("prod1", "t1", e);
        assertTrue(tributary.getPartition("t1", "p2").getEvents().contains(e));
    }

    @Test
    public void testIncompatibleProduce() {
        Tributary tributary = new Tributary();

        tributary.createTopic("t1", "integer");
        tributary.createPartition("p1", "t1");
        tributary.createPartition("p2", "t1");
        tributary.createProducer("prod1", "integer", new RandomMessage());

        Event<String> e = new Event<String>("event1", "string", "p2", "valueee");
        tributary.produceEvent("prod1", "t1", e);
        tributary.showTopic("t1");
        String actualOutput = outContent.toString().trim();
        assertFalse(actualOutput.contains("event1"));
        assertTrue(errContent.toString().trim().contains("Event and producer/topic types are different"));
    }

    private Tributary initialise(Tributary tributary) {
        tributary.createTopic("t1", "string");
        tributary.createPartition("p1", "t1");
        tributary.createPartition("p2", "t1");
        tributary.createPartition("p3", "t1");
        tributary.createPartition("p4", "t1");
        tributary.createProducer("prod1", "string", new ManualMessage());
        tributary.createConsumerGroup("g1", "t1", new RangeStrategy());
        tributary.createConsumer("c1", "g1");

        return tributary;
    }

    @Test
    public void consumeTest() {
        Tributary tributary = new Tributary();
        initialise(tributary);

        Event<String> e1 = new Event<String>("event1", "string", "p1", "val");
        Event<String> e2 = new Event<String>("event2", "string", "p1", "val");

        tributary.produceEvent("prod1", "t1", e1);
        tributary.produceEvent("prod1", "t1", e2);

        tributary.consumeEvents("c1", "p1", 2);

        Consumer c = tributary.getConsumer("c1");
        assertTrue(c.getConsumedEvents().contains(e1));
        assertTrue(c.getConsumedEvents().contains(e2));

        tributary.consumeEvents("c1", "p1", 1);
        assertTrue(errContent.toString().trim().contains("Error: No events left to be consumed"));
    }

    @Test
    public void replayTest() {
        Tributary tributary = new Tributary();
        initialise(tributary);

        Event<String> e1 = new Event<String>("event1", "string", "p1", "val");
        Event<String> e2 = new Event<String>("event2", "string", "p1", "val");
        Event<String> e3 = new Event<String>("event3", "string", "p1", "val");
        tributary.produceEvent("prod1", "t1", e1);
        tributary.produceEvent("prod1", "t1", e2);
        tributary.produceEvent("prod1", "t1", e3);

        tributary.consumeEvents("c1", "p1", 2);
        tributary.playback("c1", "p1", 0);

        Consumer c = tributary.getConsumer("c1");

        List<Event<?>> events = c.getConsumedEvents();
        int e1count = 0;
        int e2count = 0;

        for (Event<?> item : events) {
            if (item.equals(e1)) {
                e1count++;
            }
            if (item.equals(e2)) {
                e2count++;
            }
        }

        assertEquals(e1count, 2);
        assertEquals(e2count, 2);
        assertFalse(events.contains(e3));
    }

    @Test
    public void rebalancingTest() {
        // Initialize the Tributary instance and set up initial events
        Tributary tributary = new Tributary();
        initialise(tributary); // Assuming this method initializes the tributary with partitions and events
        tributary.createConsumer("c2", "g1");
        // Produce events
        Event<String> e1 = new Event<String>("event1", "string", "p2", "val");
        Event<String> e2 = new Event<String>("event2", "string", "p2", "val");
        Event<String> e3 = new Event<String>("event3", "string", "p2", "val");
        tributary.produceEvent("prod1", "t1", e1);
        tributary.produceEvent("prod1", "t1", e2);
        tributary.produceEvent("prod1", "t1", e3);

        // Consume events with consumer 'c1'
        tributary.consumeEvents("c1", "p1", 2);

        // Get references to consumers and partitions for assertions
        Consumer c = tributary.getConsumer("c1");
        Partition p1 = c.getPartition("p1");
        Partition p2 = c.getPartition("p2");
        Partition p3 = c.getPartition("p3");
        Partition p4 = c.getPartition("p4");

        Consumer c2 = tributary.getConsumer("c2");
        Partition p1c2 = c2.getPartition("p1");
        Partition p2c2 = c2.getPartition("p2");
        Partition p3c2 = c2.getPartition("p3");
        Partition p4c2 = c2.getPartition("p4");

        // Assertions for initial range strategy distribution
        assertTrue(p1 != null && p2 != null);
        assertTrue(p3 == null && p4 == null);

        assertTrue(p1c2 == null && p2c2 == null);
        assertTrue(p3c2 != null && p4c2 != null);

        // Change consumer group rebalancing strategy to RoundRobin
        tributary.setConsumerGroupRebalancing("g1", new RoundRobinStrategy());

        // Get updated references after rebalancing to RoundRobin
        Consumer rc = tributary.getConsumer("c1");
        Partition rp1 = rc.getPartition("p1");
        Partition rp2 = rc.getPartition("p2");
        Partition rp3 = rc.getPartition("p3");
        Partition rp4 = rc.getPartition("p4");

        Consumer rc2 = tributary.getConsumer("c2");
        Partition rp1c2 = rc2.getPartition("p1");
        Partition rp2c2 = rc2.getPartition("p2");
        Partition rp3c2 = rc2.getPartition("p3");
        Partition rp4c2 = rc2.getPartition("p4");

        // Assertions after changing to RoundRobin strategy
        assertTrue(rp1 != null && rp3 != null);
        assertTrue(rp2 == null && rp4 == null);

        assertTrue(rp2c2 != null && rp4c2 != null);
        assertTrue(rp1c2 == null && rp3c2 == null);

        // Delete consumer 'c2' and check remaining partitions for 'c1'
        tributary.deleteConsumer("g1", "c2");
        Consumer c3 = tributary.getConsumer("c1");
        Partition c3p1 = c3.getPartition("p1");
        Partition c3p2 = c3.getPartition("p2");
        Partition c3p3 = c3.getPartition("p3");
        Partition c3p4 = c3.getPartition("p4");

        assertTrue(c3p1 != null && c3p3 != null);
        assertTrue(c3p2 != null && c3p4 != null);
    }

    @Test
    public void errorStreamTests() {
        Tributary tributary = new Tributary();
        initialise(tributary);

        tributary.deleteConsumer("g1", "c5");
        String actualOutput = errContent.toString().trim();
        assertTrue(actualOutput.contains("consumer does not exist in group g1"));

        tributary.deleteConsumer("g1", "c1");
        actualOutput = errContent.toString().trim();
        assertTrue(actualOutput.contains("No consumers to rebalance"));

        tributary.produceEvent("p1", "t5", new Event<String>("actualOutput", "string", "p1", "st"));
        actualOutput = errContent.toString().trim();
        assertTrue(actualOutput.contains("Topic 't5' does not exist"));

        tributary.produceEvent("p1", "t1", new Event<String>("actualOutput", "string", "p1", "st"));
        actualOutput = errContent.toString().trim();
        assertTrue(actualOutput.contains("Producer p1 does not exist"));

        tributary.produceEvent("prod1", "t1", new Event<String>("actualOutput", "string", "p5", "st"));
        actualOutput = errContent.toString().trim();
        assertTrue(actualOutput.contains("Partition with id p5 does not exist"));
    }
}
