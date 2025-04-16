
# Tributary Messaging System

## Video Link:
https://drive.google.com/file/d/1LuoQsGhwo7dMEyRXmen0nNszh11jXfcF/view?usp=sharing
**Tributary** is a Java-based, OOP-driven messaging system for managing topics, producers, consumers, and consumer groups with customizable event handling strategies.

---

## 🚀 Key Features

### 1. **Modular OOP Architecture**

- **Core Components** (`tributary.core`): Encapsulates OOP abstractions like `Topic<T>`, `Producer<T>`, `Consumer<T>`, `ConsumerGroup`, and `Partition`. Built for extensibility and reusability.
- **API Layer** (`tributary.api.Tributary`): Unified interface for managing all system entities.
- **CLI Interface** (`tributary.cli.TributaryCLI`): Command-line control with intuitive commands (`create`, `produce`, `consume`, `parallel`, etc.).

### 2. **Type-Safe Event Handling**

- **Generics-Based Topics & Events**: Ensures compile-time type safety (`Topic<String>`, `Event<Integer>`).
- **Structured Events**: Includes headers (ID, payload type), key, and value. JSON-based input for flexible event definitions.

### 3. **Flexible Message Allocation**

- **Strategies**:
  - `RandomMessage`: Load-balanced random distribution.
  - `ManualMessage`: Key-based partition targeting.
- **Custom Allocators**: Extendable via the `MessageAllocation` interface.

### 4. **Consumer Group Rebalancing**

- **Built-in Strategies**:
  - `Range`: Contiguous partition assignment.
  - `Round-Robin`: Even distribution.
- **Dynamic Rebalancing**: Automatically adapts to consumer group changes.

### 5. **Parallel Processing**

- **Multithreaded CLI Support**: Parallel `produce` and `consume` commands for high throughput.
- **Thread Safety**: Synchronized operations, especially in `ManualMessage`.

### 6. **Event Playback**

- Replay from specific offsets using `Tributary.playback`, enabling fault tolerance and reprocessing.

### 7. **Robust Error Handling**

- Type mismatch checks and validations for non-existent topics/partitions.
- Detailed CLI error messages for smoother debugging.

### 8. **Testing & Validation**

- **JUnit Tests**: Cover full workflows and edge cases.
- **Output Verification**: Ensures correct system behavior and error messaging.

### 9. **JSON-Based Event Input**

- External event definitions via JSON files.
- Supports varied data types and structured headers.

### 10. **Interactive CLI**

- Command-driven experience: `create topic`, `produce event`, `consume events`, `playback`, etc.
- Built-in `help` command for usage guidance.

---

## 🛠️ Getting Started
# Compile and launch the CLI
java TributaryCLI
Example Workflow:

text
Copy
Edit
create topic t1 string
create partition p1 t1
create producer prod1 string random
produce event prod1 t1 event.json
create consumergroup g1 t1 range
create consumer c1 g1
consume events c1 p1 1

##🔮 Future Enhancements
Persistence: Event and state durability.

Networking: Distributed node support.

More Strategies: Expand allocation and rebalancing options.

Tributary demonstrates solid OOP design with a type-safe, flexible foundation for modern event-driven applications.
