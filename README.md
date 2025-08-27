# TaskQueue

A distributed task queue library backed by FoundationDB, providing reliable task execution with at-most-once semantics.

## Features

- **FIFO task processing** with visibility time support
- **At-most-once execution** per task key
- **Task versioning** with automatic latest version execution
- **Worker lease management** with configurable TTL
- **Throttling** to prevent rapid re-execution
- **Delayed task execution** with configurable delays
- **Task expiration and reclaiming** for fault tolerance
- **Retry logic** with configurable max attempts
- **Transaction support** for atomic operations
- **Built on FoundationDB** for reliability and scalability

## Installation

Add the following dependency to your project:

### Maven
```xml
<dependency>
    <groupId>io.github.panghy</groupId>
    <artifactId>taskqueue</artifactId>
    <version>0.1.0</version>
</dependency>
```

### Gradle
```gradle
implementation 'io.github.panghy:taskqueue:0.1.0'
```

## Quick Start

```java
import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.directory.DirectoryLayer;
import io.github.panghy.taskqueue.KeyedTaskQueue;
import io.github.panghy.taskqueue.TaskQueueConfig;
import io.github.panghy.taskqueue.serializers.StringSerializer;

// Initialize FoundationDB
FDB fdb = FDB.selectAPIVersion(730);
Database db = fdb.open();
DirectoryLayer directory = DirectoryLayer.getDefault();

// Create configuration
TaskQueueConfig<String, String> config = TaskQueueConfig.builder(
        db,
        directory.createOrOpen(db, List.of("myapp", "tasks")).get(),
        new StringSerializer(),
        new StringSerializer())
    .defaultTtl(Duration.ofMinutes(5))
    .maxAttempts(3)
    .defaultThrottle(Duration.ofSeconds(1))
    .build();

// Create or open the task queue
KeyedTaskQueue<String, String> queue = KeyedTaskQueue.createOrOpen(config, db).get();

// Enqueue a task
TaskKey taskKey = queue.enqueue("user-123", "process-user-data").get();

// Worker loop - claim and process tasks
while (true) {
    TaskClaim<String, String> claim = queue.awaitAndClaimTask(db).get();

    try {
        // Process the task
        String taskKey = claim.taskKey();
        String taskData = claim.task();

        System.out.println("Processing task: " + taskKey + " with data: " + taskData);

        // Your task processing logic here
        processTask(taskKey, taskData);

        // Mark task as completed
        claim.complete();

    } catch (Exception e) {
        // Mark task as failed (will be retried up to maxAttempts)
        claim.fail();
    }
}
```

### Advanced Usage

```java
// Enqueue with custom delay and TTL
queue.enqueue("delayed-task", "data", Duration.ofMinutes(10), Duration.ofHours(1)).get();

// Enqueue only if not already exists
TaskKey key = queue.enqueueIfNotExists("unique-task", "data").get();

// Use within transactions
db.runAsync(tr -> {
    return queue.enqueue(tr, "tx-task", "data");
}).get();
```

## Configuration Options

The `TaskQueueConfig` supports the following configuration options:

- **`defaultTtl`**: Default time-to-live for task claims (default: 5 minutes)
- **`maxAttempts`**: Maximum number of retry attempts per task (default: 3)
- **`defaultThrottle`**: Minimum time between task executions for the same key (default: 0)
- **`instantSource`**: Custom time source for testing (default: system time)

## API Reference

### KeyedTaskQueue Methods

- **`enqueue(key, task)`**: Enqueue a task with default settings
- **`enqueue(key, task, delay, ttl)`**: Enqueue a task with custom delay and TTL
- **`enqueueIfNotExists(key, task)`**: Enqueue only if no task exists for the key
- **`awaitAndClaimTask()`**: Wait for and claim the next available task
- **`completeTask(claim)`**: Mark a claimed task as completed
- **`failTask(claim)`**: Mark a claimed task as failed (will be retried)

### TaskClaim Methods

- **`complete()`**: Convenience method to complete the task
- **`fail()`**: Convenience method to fail the task
- **`taskKey()`**: Get the task key
- **`task()`**: Get the task data
- **`taskProto()`**: Get the internal task protocol buffer

## Requirements

- Java 17 or higher
- FoundationDB 7.3 or higher

## Building from Source

```bash
./gradlew clean build
```

## Testing

The project includes comprehensive tests covering all functionality:

```bash
# Run all tests
./gradlew test

# Run only KeyedTaskQueue tests
./gradlew test --tests KeyedTaskQueueTest

# Run tests with coverage report
./gradlew test jacocoTestReport
```

### Test Coverage

The test suite includes comprehensive coverage of:

- **Basic Operations**: Enqueue, claim, complete, and fail tasks
- **Task Versioning**: Multiple versions of the same task key
- **Delayed Execution**: Tasks with custom delays and timing
- **Expiration Handling**: TTL expiration and task reclaiming
- **Retry Logic**: Max attempts and failure handling
- **Concurrency**: Multiple workers processing tasks simultaneously
- **Edge Cases**: Idempotent operations, transaction contexts, and error scenarios
- **Configuration**: Custom TTL, delays, throttling, and max attempts

All tests use simulated time (no `Thread.sleep()`) for reliable and fast execution.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.