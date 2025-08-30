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
- **OpenTelemetry instrumentation** for distributed tracing and metrics
- **Built on FoundationDB** for reliability and scalability

## Installation

Add the following dependency to your project:

### Maven
```xml
<dependency>
    <groupId>io.github.panghy</groupId>
    <artifactId>taskqueue</artifactId>
    <version>0.2.0</version>
</dependency>
```

### Gradle
```gradle
implementation 'io.github.panghy:taskqueue:0.2.0'
```

## Quick Start

```java
import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.directory.DirectoryLayer;
import io.github.panghy.taskqueue.KeyedTaskQueue;
import io.github.panghy.taskqueue.TaskQueueConfig;
import io.github.panghy.taskqueue.serializers.StringSerializer;
import java.util.concurrent.CompletableFuture;

// Initialize FoundationDB
FDB fdb = FDB.selectAPIVersion(730);
Database db = fdb.open();
DirectoryLayer directory = DirectoryLayer.getDefault();

// Create configuration asynchronously
directory.createOrOpen(db, List.of("myapp", "tasks"))
    .thenCompose(dirSubspace -> {
        TaskQueueConfig<String, String> config = TaskQueueConfig.builder(
                db,
                dirSubspace,
                new StringSerializer(),
                new StringSerializer())
            .defaultTtl(Duration.ofMinutes(5))
            .maxAttempts(3)
            .defaultThrottle(Duration.ofSeconds(1))
            .build();
        
        // Create or open the task queue
        return KeyedTaskQueue.createOrOpen(config, db);
    })
    .thenCompose(queue -> {
        // Enqueue a task
        return queue.enqueue("user-123", "process-user-data")
            .thenApply(taskKey -> {
                System.out.println("Task enqueued: " + taskKey);
                return queue;
            });
    })
    .thenCompose(queue -> {
        // Start worker loop
        return processTasksAsync(queue, db);
    });

// Async worker method
private CompletableFuture<Void> processTasksAsync(
        KeyedTaskQueue<String, String> queue, 
        Database db) {
    
    return queue.awaitAndClaimTask(db)
        .thenCompose(claim -> {
            // Process the task
            String taskKey = claim.taskKey();
            String taskData = claim.task();
            
            System.out.println("Processing task: " + taskKey + " with data: " + taskData);
            
            // Your async task processing logic here
            return processTaskAsync(taskKey, taskData)
                .thenCompose(result -> {
                    // Mark task as completed
                    System.out.println("Task completed: " + taskKey);
                    return claim.complete();
                })
                .exceptionallyCompose(error -> {
                    // Mark task as failed (will be retried up to maxAttempts)
                    System.err.println("Task failed: " + error.getMessage());
                    // Properly chain the fail operation without blocking
                    return claim.fail();
                });
        })
        .thenCompose(v -> {
            // Continue processing next task
            return processTasksAsync(queue, db);
        });
}

// Example async task processor
private CompletableFuture<String> processTaskAsync(String key, String data) {
    return CompletableFuture.supplyAsync(() -> {
        // Simulate async work
        return "Processed: " + key + " - " + data;
    });
}
```

### Advanced Usage

```java
// Enqueue with custom delay and TTL
queue.enqueue("delayed-task", "data", Duration.ofMinutes(10), Duration.ofHours(1))
    .thenAccept(taskKey -> {
        System.out.println("Delayed task scheduled: " + taskKey);
    });

// Enqueue only if not already exists
queue.enqueueIfNotExists("unique-task", "data")
    .thenAccept(taskKey -> {
        if (taskKey != null) {
            System.out.println("New task created: " + taskKey);
        } else {
            System.out.println("Task already exists");
        }
    });

// Use within transactions
db.runAsync(tr -> {
    return queue.enqueue(tr, "tx-task", "data")
        .thenCompose(taskKey -> {
            // Perform other transactional operations
            return someOtherOperation(tr)
                .thenApply(result -> taskKey);
        });
})
.thenAccept(taskKey -> {
    System.out.println("Transaction completed, task: " + taskKey);
});

// Parallel task processing with multiple workers
CompletableFuture<?>[] workers = new CompletableFuture[5];
for (int i = 0; i < workers.length; i++) {
    final int workerId = i;
    workers[i] = processWorkerAsync(queue, db, workerId);
}

// Wait for all workers (or use CompletableFuture.allOf())
CompletableFuture.allOf(workers)
    .exceptionally(error -> {
        System.err.println("Worker error: " + error);
        return null;
    });

// Extend TTL for long-running tasks
queue.awaitAndClaimTask(db)
    .thenCompose(claim -> {
        // Start processing
        return startLongRunningTask(claim.task())
            .thenCompose(intermediate -> {
                // Extend TTL midway through processing
                return claim.extend(Duration.ofMinutes(10))
                    .thenCompose(v -> continueLongRunningTask(intermediate));
            })
            .thenCompose(result -> {
                // Complete the task
                return claim.complete();
            });
    });
```

## Configuration Options

The `TaskQueueConfig` supports the following configuration options:

- **`defaultTtl`**: Default time-to-live for task claims (default: 5 minutes)
- **`maxAttempts`**: Maximum number of retry attempts per task (default: 3)
- **`defaultThrottle`**: Minimum time between task executions for the same key (default: 0)
- **`instantSource`**: Custom time source for testing (default: system time)

## Observability with OpenTelemetry

TaskQueue includes built-in OpenTelemetry instrumentation for distributed tracing and metrics collection. All major operations are automatically instrumented with spans and metrics.

### Tracing

The following operations create spans:
- `enqueue` - Tracks task enqueuing with task key and delay attributes
- `awaitAndClaimTask` - Tracks the claim process including wait time
- `completeTask` - Tracks task completion with processing duration
- `failTask` - Tracks task failures with error details
- `extendTtl` - Tracks TTL extension operations

### Metrics

The following metrics are automatically collected:
- `taskqueue.tasks.enqueued` - Counter of tasks enqueued
- `taskqueue.tasks.claimed` - Counter of tasks claimed by workers
- `taskqueue.tasks.completed` - Counter of successfully completed tasks
- `taskqueue.tasks.failed` - Counter of failed tasks
- `taskqueue.task.processing.duration` - Histogram of task processing time (ms)
- `taskqueue.task.wait.time` - Histogram of time tasks wait before being claimed (ms)

### Setup

To enable observability, configure OpenTelemetry in your application:

```java
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;

// Configure OpenTelemetry SDK (example with OTLP exporter)
OpenTelemetrySdk openTelemetry = OpenTelemetrySdk.builder()
    .setTracerProvider(SdkTracerProvider.builder()
        // Add your span processor/exporter here
        .build())
    .setMeterProvider(SdkMeterProvider.builder()
        // Add your metric reader/exporter here
        .build())
    .buildAndRegisterGlobal();

// TaskQueue will automatically use the global OpenTelemetry instance
```

## API Reference

### KeyedTaskQueue Methods

All methods return `CompletableFuture` for async operations:

- **`enqueue(key, task)`**: Enqueue a task with default settings → `CompletableFuture<TaskKeyMetadata>`
- **`enqueue(key, task, delay, ttl)`**: Enqueue a task with custom delay and TTL → `CompletableFuture<TaskKeyMetadata>`
- **`enqueueIfNotExists(key, task)`**: Enqueue only if no task exists for the key → `CompletableFuture<TaskKeyMetadata>`
- **`awaitAndClaimTask(db)`**: Wait for and claim the next available task → `CompletableFuture<TaskClaim<K,T>>`
- **`completeTask(claim)`**: Mark a claimed task as completed → `CompletableFuture<Void>`
- **`failTask(claim)`**: Mark a claimed task as failed (will be retried) → `CompletableFuture<Void>`
- **`extendTtl(claim, duration)`**: Extend the TTL of a claimed task → `CompletableFuture<Void>`

### TaskClaim Methods

Convenience methods that return `CompletableFuture` for async operations:

- **`complete()`**: Complete the task → `CompletableFuture<Void>`
- **`fail()`**: Fail the task → `CompletableFuture<Void>`
- **`extend(duration)`**: Extend the TTL → `CompletableFuture<Void>`
- **`taskKey()`**: Get the task key (synchronous)
- **`task()`**: Get the task data (synchronous)
- **`getAttempts()`**: Get the number of attempts (synchronous)
- **`getExpirationTime()`**: Get when this claim expires (synchronous)
- **`isExpired()`**: Check if the claim has expired (synchronous)

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