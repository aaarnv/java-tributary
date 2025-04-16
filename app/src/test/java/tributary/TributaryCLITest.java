package tributary;

import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;
import java.io.*;
import tributary.cli.TributaryCLI;

public class TributaryCLITest {
    private final PrintStream originalOut = System.out;
    private final PrintStream originalErr = System.err;
    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
    private TributaryCLI tributaryCLI;

    @BeforeEach
    public void setUpStreams() {
        System.setOut(new PrintStream(outContent));
        System.setErr(new PrintStream(errContent));
        tributaryCLI = new TributaryCLI();
    }

    @AfterEach
    public void restoreStreams() {
        System.setOut(originalOut);
        System.setErr(originalErr);
    }

    @Test
    public void testHelpCommand() {
        tributaryCLI.processCommand("help");
        assertTrue(outContent.toString().contains("Available commands"));
    }

    @Test
    public void testCreateCommand() {
        tributaryCLI.processCommand("create topic t1 string");
        assertTrue(outContent.toString().contains("Topic t1 created with type string"));

        tributaryCLI.processCommand("create partition p1 t1");
        assertTrue(outContent.toString().contains("Partition 'p1' created in Topic: t1"));

        tributaryCLI.processCommand("create producer prod1 string random");
        assertTrue(outContent.toString().contains("Producer prod1 created with type string and allocation Random"));
    }

    @Test
    public void testShowCommand() {
        tributaryCLI.processCommand("create topic t1 string");
        tributaryCLI.processCommand("show topic t1");
        assertTrue(outContent.toString().contains("t1"));

        tributaryCLI.processCommand("create consumergroup g1 t1 range");
        tributaryCLI.processCommand("show consumergroup g1");
        assertTrue(outContent.toString().contains("Consumer Group ID: g1"));
    }

    @Test
    public void testProduceCommand() {
        tributaryCLI.processCommand("create topic t1 string");
        tributaryCLI.processCommand("create partition p1 t1");
        tributaryCLI.processCommand("create producer prod1 string random");
        assertTrue(outContent.toString().contains("Producer prod1 created with type string and allocation Random"));
    }

}
