package io.github.panghy.taskqueue;

import static org.assertj.core.api.Assertions.assertThat;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TaskQueuesTest {

  private Database database;
  private DirectorySubspace directory;

  @BeforeEach
  void setUp() throws ExecutionException, InterruptedException, TimeoutException {
    database = FDB.selectAPIVersion(730).open();
    directory = database.runAsync(tr -> {
          DirectoryLayer layer = DirectoryLayer.getDefault();
          return layer.createOrOpen(
              tr,
              List.of("test-queues", UUID.randomUUID().toString()),
              "task_queue".getBytes(StandardCharsets.UTF_8));
        })
        .get(5, TimeUnit.SECONDS);
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.run(tr -> {
        directory.remove(tr);
        return null;
      });
    }
  }

  @Test
  void testCreateTaskQueue() throws ExecutionException, InterruptedException {
    TaskQueueConfig<String, String> config = TaskQueueConfig.builder(
            database, directory, new StringSerializer(), new StringSerializer())
        .defaultTtl(Duration.ofMinutes(5))
        .maxAttempts(3)
        .build();

    TaskQueue<String, String> taskQueue = TaskQueues.createTaskQueue(config).get();
    assertThat(taskQueue).isNotNull();
    assertThat(taskQueue.getConfig()).isEqualTo(config);
  }

  @Test
  void testCreateSimpleTaskQueue() throws ExecutionException, InterruptedException {
    TaskQueueConfig<UUID, String> config = TaskQueueConfig.<String>builder(
            database, directory, new StringSerializer())
        .defaultTtl(Duration.ofMinutes(5))
        .maxAttempts(3)
        .build();

    SimpleTaskQueue<String> simpleTaskQueue =
        TaskQueues.createSimpleTaskQueue(config).get();
    assertThat(simpleTaskQueue).isNotNull();
    assertThat(simpleTaskQueue.getConfig()).isEqualTo(config);
  }

  @Test
  void testCreateTaskQueueWithCustomKeyType() throws ExecutionException, InterruptedException {
    TaskQueueConfig<Long, String> config = TaskQueueConfig.builder(
            database, directory, new LongSerializer(), new StringSerializer())
        .defaultTtl(Duration.ofMinutes(10))
        .maxAttempts(5)
        .defaultThrottle(Duration.ofSeconds(2))
        .build();

    TaskQueue<Long, String> taskQueue = TaskQueues.createTaskQueue(config).get();
    assertThat(taskQueue).isNotNull();
    assertThat(taskQueue.getConfig()).isEqualTo(config);

    database.run(tr -> {
      try {
        taskQueue
            .enqueue(tr, 123L, "test-task", Duration.ZERO, Duration.ofMinutes(5))
            .get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
      return null;
    });
    TaskClaim<Long, String> claim = taskQueue.awaitAndClaimTask(database).get();
    assertThat(claim.taskKey()).isEqualTo(123L);
    assertThat(claim.task()).isEqualTo("test-task");
    taskQueue.completeTask(claim).get();
  }

  static class LongSerializer implements TaskQueueConfig.TaskSerializer<Long> {
    @Override
    public com.google.protobuf.ByteString serialize(Long value) {
      byte[] bytes = new byte[8];
      for (int i = 0; i < 8; i++) {
        bytes[7 - i] = (byte) (value >>> (i * 8));
      }
      return com.google.protobuf.ByteString.copyFrom(bytes);
    }

    @Override
    public Long deserialize(com.google.protobuf.ByteString bytes) {
      byte[] array = bytes.toByteArray();
      long value = 0;
      for (int i = 0; i < 8; i++) {
        value = (value << 8) | (array[i] & 0xff);
      }
      return value;
    }
  }
}
