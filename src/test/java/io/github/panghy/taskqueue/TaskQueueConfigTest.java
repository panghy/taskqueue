package io.github.panghy.taskqueue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TaskQueueConfigTest {

  Database db;
  DirectorySubspace directory;
  TaskQueueConfig<String, String> config;
  AtomicLong simulatedTime = new AtomicLong(0);

  @BeforeEach
  void setup() throws ExecutionException, InterruptedException, TimeoutException {
    db = FDB.selectAPIVersion(730).open();
    directory = db.runAsync(tr -> {
          DirectoryLayer layer = DirectoryLayer.getDefault();
          return layer.createOrOpen(
              tr,
              List.of("test", UUID.randomUUID().toString()),
              "task_queue".getBytes(StandardCharsets.UTF_8));
        })
        .get(5, TimeUnit.SECONDS);
    config = TaskQueueConfig.builder(db, directory, new StringSerializer(), new StringSerializer())
        .instantSource(() -> Instant.ofEpochMilli(simulatedTime.get()))
        .build();
  }

  @AfterEach
  void tearDown() {
    db.run(tr -> {
      directory.remove(tr);
      return null;
    });
    db.close();
  }

  @Test
  void testBuilderDefaults() {
    TaskQueueConfig<String, String> config = TaskQueueConfig.builder(
            db, directory, new StringSerializer(), new StringSerializer())
        .build();

    assertEquals(Duration.ofMinutes(5), config.getDefaultTtl());
    assertEquals(3, config.getMaxAttempts());
    assertEquals(Duration.ofSeconds(1), config.getDefaultThrottle());
  }

  @Test
  void testBuilderCustomValues() {
    TaskQueueConfig<String, String> config = TaskQueueConfig.builder(
            db, directory, new StringSerializer(), new StringSerializer())
        .directory(directory)
        .defaultTtl(Duration.ofMinutes(10))
        .maxAttempts(5)
        .defaultThrottle(Duration.ofSeconds(5))
        .keySerializer(new StringSerializer())
        .taskSerializer(new StringSerializer())
        .build();

    assertEquals(Duration.ofMinutes(10), config.getDefaultTtl());
    assertEquals(5, config.getMaxAttempts());
    assertEquals(Duration.ofSeconds(5), config.getDefaultThrottle());
  }

  @Test
  void testInvalidMaxAttempts() {
    assertThrows(IllegalArgumentException.class, () -> TaskQueueConfig.builder(
            db, directory, new StringSerializer(), new StringSerializer())
        .maxAttempts(0)
        .build());
  }

  @Test
  void testInvalidTtlZero() {
    assertThrows(IllegalArgumentException.class, () -> TaskQueueConfig.builder(
            db, directory, new StringSerializer(), new StringSerializer())
        .defaultTtl(Duration.ZERO)
        .build());
  }

  @Test
  void testInvalidTtlNegative() {
    assertThrows(IllegalArgumentException.class, () -> TaskQueueConfig.builder(
            db, directory, new StringSerializer(), new StringSerializer())
        .defaultTtl(Duration.ofMinutes(-1))
        .build());
  }

  @Test
  void testInvalidThrottleNegative() {
    assertThrows(IllegalArgumentException.class, () -> TaskQueueConfig.builder(
            db, directory, new StringSerializer(), new StringSerializer())
        .defaultThrottle(Duration.ofSeconds(-1))
        .build());
  }
}
