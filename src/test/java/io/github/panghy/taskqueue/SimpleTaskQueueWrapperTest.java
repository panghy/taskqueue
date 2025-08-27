package io.github.panghy.taskqueue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SimpleTaskQueueWrapperTest {

  private Database database;
  private DirectorySubspace directory;
  private SimpleTaskQueue<String> taskQueue;
  private TaskQueueConfig<UUID, String> config;

  @BeforeEach
  void setUp() throws ExecutionException, InterruptedException, TimeoutException {
    database = FDB.selectAPIVersion(730).open();
    directory = database.runAsync(tr -> {
          DirectoryLayer layer = DirectoryLayer.getDefault();
          return layer.createOrOpen(
              tr,
              List.of("test-simple", UUID.randomUUID().toString()),
              "task_queue".getBytes(StandardCharsets.UTF_8));
        })
        .get(5, TimeUnit.SECONDS);
    config = TaskQueueConfig.<String>builder(database, directory, new StringSerializer())
        .defaultTtl(Duration.ofMinutes(5))
        .maxAttempts(3)
        .defaultThrottle(Duration.ofSeconds(1))
        .taskNameExtractor(task -> "task-" + task)
        .estimatedWorkerCount(2)
        .instantSource(InstantSource.system())
        .build();
    taskQueue = TaskQueues.createSimpleTaskQueue(config).get();
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
  void testGetConfig() {
    assertThat(taskQueue.getConfig()).isEqualTo(config);
  }

  @Test
  void testEnqueueAndAwaitAndClaim() throws ExecutionException, InterruptedException {
    taskQueue.enqueue("test-task-1").get();

    TaskClaim<UUID, String> claim = taskQueue.awaitAndClaimTask().get();
    assertThat(claim).isNotNull();
    assertThat(claim.task()).isEqualTo("test-task-1");
    assertThat(claim.taskProto().getAttempts()).isEqualTo(1);

    taskQueue.completeTask(claim).get();
  }

  @Test
  void testEnqueueWithDelay() throws ExecutionException, InterruptedException {
    // Create a config with controlled time for testing delays
    var testTime = new AtomicLong(1000);
    var testConfig = TaskQueueConfig.<String>builder(database, directory, new StringSerializer())
        .instantSource(() -> Instant.ofEpochMilli(testTime.get()))
        .defaultTtl(Duration.ofMinutes(5))
        .build();
    taskQueue = TaskQueues.createSimpleTaskQueue(testConfig).get();

    database.run(tr -> {
      try {
        taskQueue.enqueue(tr, "delayed-task", Duration.ofHours(1)).get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
      return null;
    });

    taskQueue.enqueue("immediate-task").get();

    TaskClaim<UUID, String> claim = taskQueue.awaitAndClaimTask().get();
    assertThat(claim.task()).isEqualTo("immediate-task");
    taskQueue.completeTask(claim).get();
  }

  @Test
  void testEnqueueWithCustomTtl() throws ExecutionException, InterruptedException {
    database.run(tr -> {
      try {
        taskQueue
            .enqueue(tr, "custom-ttl-task", Duration.ZERO, Duration.ofHours(1))
            .get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
      return null;
    });

    TaskClaim<UUID, String> claim = taskQueue.awaitAndClaimTask().get();
    assertThat(claim).isNotNull();
    assertThat(claim.task()).isEqualTo("custom-ttl-task");

    taskQueue.completeTask(claim).get();
  }

  @Test
  void testFailTask() throws ExecutionException, InterruptedException {
    taskQueue.enqueue("fail-task").get();

    TaskClaim<UUID, String> claim = taskQueue.awaitAndClaimTask().get();
    assertThat(claim.task()).isEqualTo("fail-task");

    taskQueue.failTask(claim).get();

    TaskClaim<UUID, String> reclaimedTask = taskQueue.awaitAndClaimTask().get();
    assertThat(reclaimedTask.task()).isEqualTo("fail-task");
    assertThat(reclaimedTask.taskProto().getAttempts()).isEqualTo(2);

    taskQueue.completeTask(reclaimedTask).get();
  }

  @Test
  void testRunAsync() throws ExecutionException, InterruptedException {
    String result = taskQueue
        .runAsync(tr -> {
          return CompletableFuture.completedFuture("test-result");
        })
        .get();
    assertThat(result).isEqualTo("test-result");
  }

  @Test
  void testMultipleTasks() throws ExecutionException, InterruptedException {
    taskQueue.enqueue("task-1").get();
    taskQueue.enqueue("task-2").get();
    taskQueue.enqueue("task-3").get();

    TaskClaim<UUID, String> claim1 = taskQueue.awaitAndClaimTask().get();
    assertThat(claim1.task()).isIn("task-1", "task-2", "task-3");
    taskQueue.completeTask(claim1).get();

    TaskClaim<UUID, String> claim2 = taskQueue.awaitAndClaimTask().get();
    assertThat(claim2.task()).isIn("task-1", "task-2", "task-3");
    taskQueue.completeTask(claim2).get();

    TaskClaim<UUID, String> claim3 = taskQueue.awaitAndClaimTask().get();
    assertThat(claim3.task()).isIn("task-1", "task-2", "task-3");
    taskQueue.completeTask(claim3).get();
  }

  @Test
  void testEnqueueInTransaction() throws ExecutionException, InterruptedException {
    database.run(tr -> {
      try {
        taskQueue.enqueue(tr, "transactional-task").get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
      return null;
    });

    TaskClaim<UUID, String> claim = taskQueue.awaitAndClaimTask().get();
    assertThat(claim.task()).isEqualTo("transactional-task");

    database.run(tr -> {
      try {
        taskQueue.completeTask(tr, claim).get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
      return null;
    });
  }

  @Test
  void testFailTaskInTransaction() throws ExecutionException, InterruptedException {
    taskQueue.enqueue("fail-in-tx").get();

    TaskClaim<UUID, String> claim = taskQueue.awaitAndClaimTask().get();
    assertThat(claim.task()).isEqualTo("fail-in-tx");

    database.run(tr -> {
      try {
        taskQueue.failTask(tr, claim).get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
      return null;
    });

    TaskClaim<UUID, String> reclaimedTask = taskQueue.awaitAndClaimTask().get();
    assertThat(reclaimedTask.task()).isEqualTo("fail-in-tx");
    assertThat(reclaimedTask.taskProto().getAttempts()).isEqualTo(2);

    taskQueue.completeTask(reclaimedTask).get();
  }

  @Test
  void testExtendTtlSuccessfully() throws ExecutionException, InterruptedException {
    taskQueue.enqueue("task-to-extend").get();

    TaskClaim<UUID, String> claim = taskQueue.awaitAndClaimTask().get();
    assertThat(claim.task()).isEqualTo("task-to-extend");

    // Extend the TTL
    taskQueue.extendTtl(claim, Duration.ofMinutes(10)).get();

    taskQueue.completeTask(claim).get();
  }

  @Test
  void testExtendTtlUsingConvenienceMethod() throws ExecutionException, InterruptedException {
    taskQueue.enqueue("task-with-convenience").get();

    TaskClaim<UUID, String> claim = taskQueue.awaitAndClaimTask().get();
    assertThat(claim.task()).isEqualTo("task-with-convenience");

    // Use convenience method
    claim.extend(Duration.ofMinutes(20));

    taskQueue.completeTask(claim).get();
  }

  @Test
  void testExtendTtlWithInvalidDuration() throws ExecutionException, InterruptedException {
    taskQueue.enqueue("test-invalid").get();

    TaskClaim<UUID, String> claim = taskQueue.awaitAndClaimTask().get();

    assertThatThrownBy(
            () -> taskQueue.extendTtl(claim, Duration.ofMinutes(-5)).get())
        .isInstanceOf(ExecutionException.class)
        .hasCauseInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must be positive");

    taskQueue.completeTask(claim).get();
  }

  @Test
  void testAwaitAndClaimWithDatabase() throws ExecutionException, InterruptedException {
    taskQueue.enqueue("test-with-db").get();

    TaskClaim<UUID, String> claim = taskQueue.awaitAndClaimTask(database).get();
    assertThat(claim).isNotNull();
    assertThat(claim.task()).isEqualTo("test-with-db");

    taskQueue.completeTask(claim).get();
  }
}
