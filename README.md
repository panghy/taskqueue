# TaskQueue

A distributed task queue library backed by FoundationDB, providing reliable task execution with at-most-once semantics.

## Features

- **FIFO task processing** with visibility time support
- **At-most-once execution** per task key
- **Task versioning** with automatic latest version execution
- **Worker lease management** with configurable TTL
- **Throttling** to prevent rapid re-execution
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
// Create configuration
TaskQueueConfig<String, MyTask> config = TaskQueueConfig.<String, MyTask>builder()
    .keyPrefix(Tuple.from("myapp", "tasks"))
    .defaultTtl(Duration.ofMinutes(5))
    .maxAttempts(3)
    .defaultThrottle(Duration.ofSeconds(1))
    .keySerializer(new StringSerializer())
    .taskSerializer(new MyTaskSerializer())
    .build();

// TODO: Add TaskQueue usage example once implemented
```

## Requirements

- Java 17 or higher
- FoundationDB 7.3 or higher

## Building from Source

```bash
./gradlew clean build
```

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.