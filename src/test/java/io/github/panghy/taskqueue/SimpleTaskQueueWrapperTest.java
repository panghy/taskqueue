package io.github.panghy.taskqueue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import io.github.panghy.taskqueue.proto.DeadLetteredTask;
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
  void tearDown() throws ExecutionException, InterruptedException, TimeoutException {
    if (database != null && directory != null) {
      database.runAsync(tr -> {
            directory.remove(tr);
            return CompletableFuture.completedFuture(null);
          })
          .get(5, TimeUnit.SECONDS);
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
  void testEnqueueWithDelay() throws ExecutionException, InterruptedException, TimeoutException {
    // Create a config with controlled time for testing delays
    var testTime = new AtomicLong(1000);
    var testConfig = TaskQueueConfig.<String>builder(database, directory, new StringSerializer())
        .instantSource(() -> Instant.ofEpochMilli(testTime.get()))
        .defaultTtl(Duration.ofMinutes(5))
        .build();
    taskQueue = TaskQueues.createSimpleTaskQueue(testConfig).get();

    database.runAsync(tr -> taskQueue.enqueue(tr, "delayed-task", Duration.ofHours(1)))
        .get(5, TimeUnit.SECONDS);

    taskQueue.enqueue("immediate-task").get();

    TaskClaim<UUID, String> claim = taskQueue.awaitAndClaimTask().get();
    assertThat(claim.task()).isEqualTo("immediate-task");
    taskQueue.completeTask(claim).get();
  }

  @Test
  void testEnqueueWithCustomTtl() throws ExecutionException, InterruptedException, TimeoutException {
    database.runAsync(tr -> taskQueue.enqueue(tr, "custom-ttl-task", Duration.ZERO, Duration.ofHours(1)))
        .get(5, TimeUnit.SECONDS);

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
  void testEnqueueInTransaction() throws ExecutionException, InterruptedException, TimeoutException {
    database.runAsync(tr -> taskQueue.enqueue(tr, "transactional-task")).get(5, TimeUnit.SECONDS);

    TaskClaim<UUID, String> claim = taskQueue.awaitAndClaimTask().get();
    assertThat(claim.task()).isEqualTo("transactional-task");

    database.runAsync(tr -> taskQueue.completeTask(tr, claim)).get(5, TimeUnit.SECONDS);
  }

  @Test
  void testFailTaskInTransaction() throws ExecutionException, InterruptedException, TimeoutException {
    taskQueue.enqueue("fail-in-tx").get();

    TaskClaim<UUID, String> claim = taskQueue.awaitAndClaimTask().get();
    assertThat(claim.task()).isEqualTo("fail-in-tx");

    database.runAsync(tr -> taskQueue.failTask(tr, claim)).get(5, TimeUnit.SECONDS);

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
  void testExtendTtlUsingConvenienceMethod() throws ExecutionException, InterruptedException, TimeoutException {
    taskQueue.enqueue("task-with-convenience").get();

    TaskClaim<UUID, String> claim = taskQueue.awaitAndClaimTask().get();
    assertThat(claim.task()).isEqualTo("task-with-convenience");

    // Use convenience method
    claim.extend(Duration.ofMinutes(20)).get(5, TimeUnit.SECONDS);

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

  @Test
  void testIsEmpty() throws ExecutionException, InterruptedException {
    // Queue should be empty initially
    assertThat(taskQueue.isEmpty().get()).isTrue();

    // Enqueue a task
    taskQueue.enqueue("test-task").get();
    assertThat(taskQueue.isEmpty().get()).isFalse();

    // Claim the task
    TaskClaim<UUID, String> claim = taskQueue.awaitAndClaimTask().get();
    assertThat(taskQueue.isEmpty().get()).isFalse(); // Still not empty (task is claimed)

    // Complete the task
    taskQueue.completeTask(claim).get();
    assertThat(taskQueue.isEmpty().get()).isTrue(); // Now empty again
  }

  @Test
  void testIsEmptyWithTransaction() throws ExecutionException, InterruptedException, TimeoutException {
    // Test isEmpty within a transaction
    Boolean emptyResult = database.runAsync(tr -> taskQueue.isEmpty(tr)).get(5, TimeUnit.SECONDS);
    assertThat(emptyResult).isTrue();

    // Add task and check again in transaction
    database.runAsync(tr -> {
          return taskQueue.enqueue(tr, "tx-task").thenCompose(v -> taskQueue.isEmpty(tr));
        })
        .get(5, TimeUnit.SECONDS);

    assertThat(taskQueue.isEmpty().get()).isFalse();

    // Clean up
    TaskClaim<UUID, String> claim = taskQueue.awaitAndClaimTask().get();
    taskQueue.completeTask(claim).get();
  }

  @Test
  void testHasVisibleUnclaimedTasks() throws ExecutionException, InterruptedException {
    // Queue should not have visible tasks initially
    assertThat(taskQueue.hasVisibleUnclaimedTasks().get()).isFalse();

    // Enqueue a task - should be immediately visible
    taskQueue.enqueue("test-task").get();
    assertThat(taskQueue.hasVisibleUnclaimedTasks().get()).isTrue();

    // Claim the task
    TaskClaim<UUID, String> claim = taskQueue.awaitAndClaimTask().get();
    assertThat(taskQueue.hasVisibleUnclaimedTasks().get()).isFalse(); // No unclaimed tasks

    // Fail the task to make it unclaimed again
    taskQueue.failTask(claim).get();
    assertThat(taskQueue.hasVisibleUnclaimedTasks().get()).isTrue();

    // Complete the task
    TaskClaim<UUID, String> claim2 = taskQueue.awaitAndClaimTask().get();
    taskQueue.completeTask(claim2).get();
    assertThat(taskQueue.hasVisibleUnclaimedTasks().get()).isFalse();
  }

  @Test
  void testHasVisibleUnclaimedTasksWithDelay() throws ExecutionException, InterruptedException, TimeoutException {
    // Enqueue a task with delay - should not be immediately visible
    database.runAsync(tr -> taskQueue.enqueue(tr, "delayed-task", Duration.ofMillis(500)))
        .get();
    assertThat(taskQueue.hasVisibleUnclaimedTasks().get()).isFalse();

    // Wait for the task to become visible
    Thread.sleep(1000);

    // Use awaitAndClaimTask to verify task is actually visible
    TaskClaim<UUID, String> claim = taskQueue.awaitAndClaimTask().get(5, TimeUnit.SECONDS);
    assertThat(claim).isNotNull();

    // Now check hasVisibleUnclaimedTasks with a new task
    database.runAsync(tr -> taskQueue.enqueue(tr, "task2")).get();
    assertThat(taskQueue.hasVisibleUnclaimedTasks().get()).isTrue();

    // Clean up
    taskQueue.completeTask(claim).get();
    TaskClaim<UUID, String> claim2 = taskQueue.awaitAndClaimTask().get();
    taskQueue.completeTask(claim2).get();
  }

  @Test
  void testHasVisibleUnclaimedTasksWithTransaction()
      throws ExecutionException, InterruptedException, TimeoutException {
    // Test hasVisibleUnclaimedTasks within a transaction
    Boolean hasVisible =
        database.runAsync(tr -> taskQueue.hasVisibleUnclaimedTasks(tr)).get(5, TimeUnit.SECONDS);
    assertThat(hasVisible).isFalse();

    // Add task and check again in transaction
    database.runAsync(tr -> {
          return taskQueue.enqueue(tr, "tx-task").thenCompose(v -> taskQueue.hasVisibleUnclaimedTasks(tr));
        })
        .get(5, TimeUnit.SECONDS);

    assertThat(taskQueue.hasVisibleUnclaimedTasks().get()).isTrue();

    // Clean up
    TaskClaim<UUID, String> claim = taskQueue.awaitAndClaimTask().get();
    taskQueue.completeTask(claim).get();
  }

  // ---- Dead Letter Queue (DLQ) Tests ----

  @Test
  void testDlqEnabled_tasksMoveToDlqAfterMaxAttempts()
      throws ExecutionException, InterruptedException, TimeoutException {
    var dlqConfig = TaskQueueConfig.<String>builder(database, directory, new StringSerializer())
        .instantSource(InstantSource.system())
        .maxAttempts(2)
        .dlqEnabled(true)
        .build();
    var dlqQueue = TaskQueues.createSimpleTaskQueue(dlqConfig).get();

    dlqQueue.enqueue("dlq-simple-task").get(5, TimeUnit.SECONDS);

    // First attempt - fail
    var claim1 = dlqQueue.awaitAndClaimTask().get(5, TimeUnit.SECONDS);
    dlqQueue.failTask(claim1).get(5, TimeUnit.SECONDS);

    // Second attempt - fail again (max attempts reached)
    var claim2 = dlqQueue.awaitAndClaimTask().get(5, TimeUnit.SECONDS);
    assertThat(claim2.taskProto().getAttempts()).isEqualTo(2);
    dlqQueue.failTask(claim2).get(5, TimeUnit.SECONDS);

    // Task should be in DLQ
    assertThat(dlqQueue.getDlqSize().get(5, TimeUnit.SECONDS)).isEqualTo(1);
  }

  @Test
  void testDlqFailureReasonStoredAndRetrievable() throws ExecutionException, InterruptedException, TimeoutException {
    var dlqConfig = TaskQueueConfig.<String>builder(database, directory, new StringSerializer())
        .instantSource(InstantSource.system())
        .maxAttempts(1)
        .dlqEnabled(true)
        .build();
    var dlqQueue = TaskQueues.createSimpleTaskQueue(dlqConfig).get();

    dlqQueue.enqueue("reason-task").get(5, TimeUnit.SECONDS);

    var claim = dlqQueue.awaitAndClaimTask().get(5, TimeUnit.SECONDS);
    dlqQueue.failTask(claim, "Timeout error").get(5, TimeUnit.SECONDS);

    List<DeadLetteredTask> dlqTasks = dlqQueue.listDlqTasks(10).get(5, TimeUnit.SECONDS);
    assertThat(dlqTasks).hasSize(1);
    assertThat(dlqTasks.get(0).getFailureReason()).isEqualTo("Timeout error");
  }

  @Test
  void testRedriveFromDlqByCount() throws ExecutionException, InterruptedException, TimeoutException {
    var dlqConfig = TaskQueueConfig.<String>builder(database, directory, new StringSerializer())
        .instantSource(InstantSource.system())
        .maxAttempts(1)
        .dlqEnabled(true)
        .build();
    var dlqQueue = TaskQueues.createSimpleTaskQueue(dlqConfig).get();

    // Add 2 tasks to DLQ
    dlqQueue.enqueue("task-a").get(5, TimeUnit.SECONDS);
    var claimA = dlqQueue.awaitAndClaimTask().get(5, TimeUnit.SECONDS);
    dlqQueue.failTask(claimA).get(5, TimeUnit.SECONDS);

    dlqQueue.enqueue("task-b").get(5, TimeUnit.SECONDS);
    var claimB = dlqQueue.awaitAndClaimTask().get(5, TimeUnit.SECONDS);
    dlqQueue.failTask(claimB).get(5, TimeUnit.SECONDS);

    assertThat(dlqQueue.getDlqSize().get(5, TimeUnit.SECONDS)).isEqualTo(2);

    // Redrive all
    int redriven = dlqQueue.redriveFromDlq(10).get(5, TimeUnit.SECONDS);
    assertThat(redriven).isEqualTo(2);
    assertThat(dlqQueue.getDlqSize().get(5, TimeUnit.SECONDS)).isEqualTo(0);

    // Claim redriven tasks
    var r1 = dlqQueue.awaitAndClaimTask().get(5, TimeUnit.SECONDS);
    var r2 = dlqQueue.awaitAndClaimTask().get(5, TimeUnit.SECONDS);
    dlqQueue.completeTask(r1).get(5, TimeUnit.SECONDS);
    dlqQueue.completeTask(r2).get(5, TimeUnit.SECONDS);
  }

  @Test
  void testPurgeDlq() throws ExecutionException, InterruptedException, TimeoutException {
    var dlqConfig = TaskQueueConfig.<String>builder(database, directory, new StringSerializer())
        .instantSource(InstantSource.system())
        .maxAttempts(1)
        .dlqEnabled(true)
        .build();
    var dlqQueue = TaskQueues.createSimpleTaskQueue(dlqConfig).get();

    dlqQueue.enqueue("purge-task").get(5, TimeUnit.SECONDS);
    var claim = dlqQueue.awaitAndClaimTask().get(5, TimeUnit.SECONDS);
    dlqQueue.failTask(claim).get(5, TimeUnit.SECONDS);

    assertThat(dlqQueue.getDlqSize().get(5, TimeUnit.SECONDS)).isEqualTo(1);

    dlqQueue.purgeDlq().get(5, TimeUnit.SECONDS);

    assertThat(dlqQueue.getDlqSize().get(5, TimeUnit.SECONDS)).isEqualTo(0);
  }

  @Test
  void testGetDlqSizeEmpty() throws ExecutionException, InterruptedException, TimeoutException {
    assertThat(taskQueue.getDlqSize().get(5, TimeUnit.SECONDS)).isEqualTo(0);
  }

  @Test
  void testListDlqTasksWithLimit() throws ExecutionException, InterruptedException, TimeoutException {
    var dlqConfig = TaskQueueConfig.<String>builder(database, directory, new StringSerializer())
        .instantSource(InstantSource.system())
        .maxAttempts(1)
        .dlqEnabled(true)
        .build();
    var dlqQueue = TaskQueues.createSimpleTaskQueue(dlqConfig).get();

    // Add 3 tasks to DLQ
    for (int i = 1; i <= 3; i++) {
      dlqQueue.enqueue("list-task-" + i).get(5, TimeUnit.SECONDS);
      var claim = dlqQueue.awaitAndClaimTask().get(5, TimeUnit.SECONDS);
      dlqQueue.failTask(claim).get(5, TimeUnit.SECONDS);
    }

    List<DeadLetteredTask> allTasks = dlqQueue.listDlqTasks(10).get(5, TimeUnit.SECONDS);
    assertThat(allTasks).hasSize(3);

    List<DeadLetteredTask> limitedTasks = dlqQueue.listDlqTasks(1).get(5, TimeUnit.SECONDS);
    assertThat(limitedTasks).hasSize(1);
  }

  @Test
  void testFailTaskWithNullReason() throws ExecutionException, InterruptedException, TimeoutException {
    var dlqConfig = TaskQueueConfig.<String>builder(database, directory, new StringSerializer())
        .instantSource(InstantSource.system())
        .maxAttempts(1)
        .dlqEnabled(true)
        .build();
    var dlqQueue = TaskQueues.createSimpleTaskQueue(dlqConfig).get();

    dlqQueue.enqueue("null-reason-task").get(5, TimeUnit.SECONDS);
    var claim = dlqQueue.awaitAndClaimTask().get(5, TimeUnit.SECONDS);
    dlqQueue.failTask(claim).get(5, TimeUnit.SECONDS); // null reason

    List<DeadLetteredTask> dlqTasks = dlqQueue.listDlqTasks(10).get(5, TimeUnit.SECONDS);
    assertThat(dlqTasks).hasSize(1);
    assertThat(dlqTasks.get(0).getFailureReason()).isEmpty();
  }

  @Test
  void testBatchedRedrive() throws ExecutionException, InterruptedException, TimeoutException {
    var dlqConfig = TaskQueueConfig.<String>builder(database, directory, new StringSerializer())
        .instantSource(InstantSource.system())
        .maxAttempts(1)
        .dlqEnabled(true)
        .dlqRedriveBatchSize(2)
        .build();
    var dlqQueue = TaskQueues.createSimpleTaskQueue(dlqConfig).get();

    // Add 5 tasks to DLQ
    for (int i = 1; i <= 5; i++) {
      dlqQueue.enqueue("batch-task-" + i).get(5, TimeUnit.SECONDS);
      var claim = dlqQueue.awaitAndClaimTask().get(5, TimeUnit.SECONDS);
      dlqQueue.failTask(claim).get(5, TimeUnit.SECONDS);
    }

    assertThat(dlqQueue.getDlqSize().get(5, TimeUnit.SECONDS)).isEqualTo(5);

    // Redrive all 5 — with batch size 2, this should use 3 transactions (2+2+1)
    int redriven = dlqQueue.redriveFromDlq(5).get(5, TimeUnit.SECONDS);
    assertThat(redriven).isEqualTo(5);
    assertThat(dlqQueue.getDlqSize().get(5, TimeUnit.SECONDS)).isEqualTo(0);

    // All 5 tasks should be claimable
    for (int i = 0; i < 5; i++) {
      var claim = dlqQueue.awaitAndClaimTask().get(5, TimeUnit.SECONDS);
      dlqQueue.completeTask(claim).get(5, TimeUnit.SECONDS);
    }
  }
}
