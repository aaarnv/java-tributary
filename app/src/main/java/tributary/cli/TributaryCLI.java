package tributary.cli;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.json.JSONObject;

import tributary.api.Tributary;
import tributary.core.*;

public class TributaryCLI {
    private final Tributary tributary;
    private final Scanner scanner;

    public TributaryCLI() {
        this.tributary = new Tributary();
        this.scanner = new Scanner(System.in);
    }

    public synchronized void processCommand(String input) {
        // Split the input into command and arguments
        String[] inputs = input.split(" ");
        switch (inputs[0]) {
        case "help":
            printHelp();
            break;
        case "create":
            handleCreate(inputs);
            break;
        case "produce":
            produceEvent(inputs);
            break;
        case "consume":
            consumeEvents(inputs);
            break;
        case "show":
            handleShow(inputs);
            break;
        case "set":
            handleSet(inputs);
            break;
        case "playback":
            handlePlayback(inputs);
            break;
        case "delete":
            handleDelete(inputs);
            break;
        case "parallel":
            handleParallelCommand(inputs);
            break;
        case "exit":
            System.out.println("Exiting CLI.");
            System.exit(0);
            break;
        default:
            System.out.println("Unknown command. Type 'help' for a list of commands.");
        }
        System.out.println();
    }

    public void handleParallelCommand(String[] inputs) {
        if (inputs.length < 3) {
            System.out.println("Invalid command. Usage: parallel produce/consume <arguments>");
            return;
        }
        switch (inputs[1]) {
        case "produce":
            handleParallelProduce(inputs);
            break;
        case "consume":
            handleParallelConsume(inputs);
            break;
        default:
            return;
        }
    }

    private void handleParallelProduce(String[] inputs) {
        List<Thread> threads = new ArrayList<>();

        for (int i = 2; i < inputs.length; i += 3) {
            String producerId = inputs[i];
            String topicId = inputs[i + 1];
            String event = inputs[i + 2];
            Event<?> e = convertJSONtoEvent(event);

            if (e != null) {
                Thread thread = new Thread(() -> {
                    tributary.produceEvent(producerId, topicId, e);
                });
                thread.start();
                threads.add(thread);
            }
        }

        // Wait for all threads to complete
        for (Thread thread : threads) {
            try {
                thread.join(); // Wait for each thread to complete
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.println("Thread interrupted while waiting for completion.");
            }
        }
    }

    private void handleParallelConsume(String[] inputs) {
        List<Thread> threads = new ArrayList<>();

        for (int i = 2; i < inputs.length; i += 2) {
            String consumer = inputs[i];
            String partition = inputs[i + 1];

            Thread thread = new Thread(() -> {
                tributary.consumeEvents(consumer, partition, 1);
            });
            thread.start();
            threads.add(thread);
        }

        // Wait for all threads to complete
        for (Thread thread : threads) {
            try {
                thread.join(); // Wait for each thread to complete
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.println("Thread interrupted while waiting for completion.");
            }
        }
    }

    private void handlePlayback(String[] inputs) {
        String consumerId = inputs[1];
        String partitionId = inputs[2];
        int offset = Integer.parseInt(inputs[3]);
        tributary.playback(consumerId, partitionId, offset);
    }

    private void handleDelete(String[] inputs) {
        String groupId = inputs[2];
        String consumerId = inputs[3];
        tributary.deleteConsumer(groupId, consumerId);
    }

    private void handleSet(String[] inputs) {
        if (inputs.length < 3) {
            System.out.println("Usage: set consumergroup <id> <rebalancing strategy>");
            return;
        }
        String groupId = inputs[2];
        String rebalancing = inputs[3];
        switch (rebalancing) {
        case "range":
            tributary.setConsumerGroupRebalancing(groupId, new RangeStrategy());
            break;
        case "roundrobin":
            tributary.setConsumerGroupRebalancing(groupId, new RoundRobinStrategy());
            break;
        default:
            System.err.println("incompatible rebalancing strategy");
            break;
        }
    }

    private void produceEvent(String[] inputs) {
        String producerId = inputs[2];
        String topicId = inputs[3];
        String event = inputs[4];
        Event<?> e = convertJSONtoEvent(event);
        if (e != null) {
            tributary.produceEvent(producerId, topicId, e);
        } else {
            System.err.println("error in produce event");
        }
    }

    private void consumeEvents(String[] inputs) {
        String consumerId = inputs[2];
        String partitionId = inputs[3];
        int numEvents = Integer.valueOf(inputs[4]);
        tributary.consumeEvents(consumerId, partitionId, numEvents);
    }

    private void handleCreate(String[] tokens) {
        if (tokens.length < 3) {
            System.out.println("Usage: create <entity> <id> <type>");
            return;
        }

        String entity = tokens[1];
        String arg1 = tokens[2];
        String arg2 = tokens[3];
        String arg3 = null;

        if (tokens.length > 4) {
            arg3 = tokens[4];
        }

        switch (entity) {
        case "topic":
            createTopic(arg1, arg2);
            break;
        case "partition":
            createPartition(arg1, arg2);
            break;
        case "consumergroup":
            createConsumerGroup(arg1, arg2, arg3);
            break;
        case "consumer":
            createConsumer(arg1, arg2);
            break;
        case "producer":
            createProducer(arg1, arg2, arg3);
            break;
        default:
            break;
        }
    }

    private void handleShow(String[] tokens) {
        if (tokens.length < 3) {
            System.out.println("Usage: show <entity> <id>");
            return;
        }

        String entity = tokens[1];
        String id = tokens[2];
        switch (entity) {
        case "topic":
            tributary.showTopic(id);
            break;
        case "consumergroup":
            tributary.showConsumerGroup(id);
            break;
        default:
            System.out.println("Unknown entity: " + entity);
        }
    }

    private void createTopic(String id, String type) {
        tributary.createTopic(id, type);
    }

    private void createPartition(String id, String topicId) {
        tributary.createPartition(id, topicId);
    }

    private void createConsumer(String id, String groupId) {
        tributary.createConsumer(id, groupId);
    }

    private void createConsumerGroup(String groupId, String id, String rebalancing) {
        RebalancingStrategy strategy = null;
        switch (rebalancing) {
        case "range":
            strategy = new RangeStrategy();
            break;
        case "roundrobin":
            strategy = new RoundRobinStrategy();
            break;
        default:
            System.err.println("unknown rebalancing strategy");
            break;
        }
        tributary.createConsumerGroup(groupId, id, strategy);
    }

    private void createProducer(String id, String type, String allocation) {
        MessageAllocation strategy = null;
        switch (allocation) {
        case "random":
            strategy = new RandomMessage();
            break;
        case "manual":
            strategy = new ManualMessage();
            break;
        default:
            System.err.println("unknown rebalancing strategy");
            break;
        }
        tributary.createProducer(id, type, strategy);
    }

    public Event<?> convertJSONtoEvent(String file) {

        String jsonString;
        try {
            File f = new File(file);
            jsonString = new String(Files.readAllBytes(f.toPath()));
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }

        JSONObject jsonObject = new JSONObject(jsonString);

        JSONObject headersJson = jsonObject.getJSONObject("headers");
        String id = headersJson.getString("id");
        String payload = headersJson.getString("payload");
        String key = jsonObject.getString("key");
        String value = jsonObject.getString("value");

        Event<?> event = null;
        switch (payload) {
        case "string":
            event = new Event<String>(id, payload, key, value);
            break;
        case "integer":
            event = new Event<Integer>(id, payload, key, Integer.parseInt(value));
            break;
        default:
            System.err.println("Invalid type of payload");
            return null;
        }

        return event;
    }

    private void printHelp() {
        System.out.println("Available commands:");
        System.out.println("create topic <id> <type>");
        System.out.println("create partition <id> <topicid>");
        System.out.println("create consumergroup <id> <topicid> <rebalancing strategy>");
        System.out.println("create consumer <id> <groupid>");
        System.out.println("create producer <id> <type> <message allocation strategy>");
        System.out.println("delete consumer <groupid> <id>");
        System.out.println("produce event <producerid> <topicid> <event>");
        System.out.println("consume events <consumerid> <partitionid> <number of events>");
        System.out.println("show topic <topicid>");
        System.out.println("show consumergroup <groupid>");
        System.out.println("set consumergroup <groupid> <rebalancing>");
        System.out.println("parallel produce (<producerid> <topicid> <event>) ...");
        System.out.println("parallel consume <consumerid> <partitionid> ...");
        System.out.println("playback <consumerid> <partitionid> <offset>");

        System.out.println("exit - Exit the CLI.");
    }

    public void takeInputs() {
        while (true) {
            System.out.print("> ");
            String input = scanner.nextLine();
            processCommand(input);
        }
    }

    public static void main(String[] args) {
        System.out.println("Welcome to the Tributary CLI!");
        System.out.println("Type 'help' for a list of commands.");
        TributaryCLI tributaryCLI = new TributaryCLI();
        tributaryCLI.takeInputs();
    }
}
