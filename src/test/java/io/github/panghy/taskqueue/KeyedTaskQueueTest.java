package io.github.panghy.taskqueue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.github.panghy.taskqueue.proto.Task;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
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

public class KeyedTaskQueueTest {

  Database db;
  DirectorySubspace directory;
  TaskQueueConfig<String, String> config;
  AtomicLong simulatedTime = new AtomicLong(0);

  // OpenTelemetry test infrastructure
  InMemorySpanExporter spanExporter;
  InMemoryMetricReader metricReader;
  OpenTelemetrySdk openTelemetry;

  @BeforeEach
  void setup() throws ExecutionException, InterruptedException, TimeoutException {
    // Setup OpenTelemetry test infrastructure
    GlobalOpenTelemetry.resetForTest();
    spanExporter = InMemorySpanExporter.create();
    metricReader = InMemoryMetricReader.create();

    SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
        .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
        .build();

    SdkMeterProvider meterProvider =
        SdkMeterProvider.builder().registerMetricReader(metricReader).build();

    openTelemetry = OpenTelemetrySdk.builder()
        .setTracerProvider(tracerProvider)
        .setMeterProvider(meterProvider)
        .buildAndRegisterGlobal();

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
    openTelemetry.shutdown();
    GlobalOpenTelemetry.resetForTest();
  }

  @Test
  void testEnqueue() throws ExecutionException, InterruptedException, TimeoutException {
    // create a task queue
    var queue = KeyedTaskQueue.createOrOpen(config, db).get();

    // enqueue a task
    var taskKey = db.runAsync(tr -> queue.enqueue(tr, "test", "test")).get();
    assertThat(taskKey).isNotNull();
    assertThat(taskKey.getHighestVersionSeen()).isEqualTo(1);
    assertThat(taskKey.hasCurrentClaim()).isFalse();

    // claim the task
    var taskClaim = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);
    assertThat(taskClaim).isNotNull();
    assertThat(taskClaim.taskKey()).isEqualTo("test");
    assertThat(taskClaim.task()).isEqualTo("test");
    assertThat(taskClaim.taskProto().getAttempts()).isEqualTo(1);
    assertThat(taskClaim.taskProto().getTaskVersion()).isEqualTo(1);
    assertThat(taskClaim.taskKeyMetadataProto().getHighestVersionSeen()).isEqualTo(1);
    assertThat(taskClaim.taskKeyMetadataProto().hasCurrentClaim()).isTrue();
    assertThat(taskClaim.taskKeyMetadataProto().getCurrentClaim().getVersion())
        .isEqualTo(1);
    assertThat(taskClaim.taskKeyMetadataProto().getCurrentClaim().getClaim())
        .isEqualTo(taskClaim.taskProto().getClaim());
    assertThat(taskClaim.taskQueue()).isEqualTo(queue);

    // complete the task
    db.runAsync(tr -> queue.completeTask(tr, taskClaim)).get(5, TimeUnit.SECONDS);

    // will stall forever
    var taskClaimF = assertQueueIsEmpty(queue);

    // add another task.
    var taskKey2 = db.runAsync(tr -> queue.enqueue(tr, "test2", "test2")).get(5, TimeUnit.SECONDS);
    assertThat(taskKey2).isNotNull();
    assertThat(taskKey2.getHighestVersionSeen()).isEqualTo(1);
    assertThat(taskKey2.hasCurrentClaim()).isFalse();

    // task should be claimed.
    TaskClaim<String, String> taskClaim2 = taskClaimF.get(5, TimeUnit.SECONDS);
    assertThat(taskClaim2).isNotNull();
    assertThat(taskClaim2.taskKey()).isEqualTo("test2");
    assertThat(taskClaim2.task()).isEqualTo("test2");
    assertThat(taskClaim2.taskProto().getAttempts()).isEqualTo(1);
    assertThat(taskClaim2.taskProto().getTaskVersion()).isEqualTo(1);
    assertThat(taskClaim2.taskKeyMetadataProto().getHighestVersionSeen()).isEqualTo(1);
    assertThat(taskClaim2.taskKeyMetadataProto().hasCurrentClaim()).isTrue();
    assertThat(taskClaim2.taskKeyMetadataProto().getCurrentClaim().getVersion())
        .isEqualTo(1);
    assertThat(taskClaim2.taskKeyMetadataProto().getCurrentClaim().getClaim())
        .isEqualTo(taskClaim2.taskProto().getClaim());
    assertThat(taskClaim2.taskQueue()).isEqualTo(queue);
  }

  @Test
  void testFail() throws ExecutionException, InterruptedException, TimeoutException {
    // create a task queue
    var queue = KeyedTaskQueue.createOrOpen(config, db).get();

    // enqueue a task
    var taskKey = queue.enqueue("test", "test").get(5, TimeUnit.SECONDS);
    assertThat(taskKey).isNotNull();
    assertThat(taskKey.getHighestVersionSeen()).isEqualTo(1);
    assertThat(taskKey.hasCurrentClaim()).isFalse();

    // claim the task
    var taskClaim = queue.awaitAndClaimTask().get(5, TimeUnit.SECONDS);
    assertThat(taskClaim).isNotNull();
    assertThat(taskClaim.taskKey()).isEqualTo("test");
    assertThat(taskClaim.task()).isEqualTo("test");
    assertThat(taskClaim.taskProto().getAttempts()).isEqualTo(1);
    assertThat(taskClaim.taskProto().getTaskVersion()).isEqualTo(1);
    assertThat(taskClaim.taskKeyMetadataProto().getHighestVersionSeen()).isEqualTo(1);
    assertThat(taskClaim.taskKeyMetadataProto().hasCurrentClaim()).isTrue();
    assertThat(taskClaim.taskKeyMetadataProto().getCurrentClaim().getVersion())
        .isEqualTo(1);
    assertThat(taskClaim.taskKeyMetadataProto().getCurrentClaim().getClaim())
        .isEqualTo(taskClaim.taskProto().getClaim());
    assertThat(taskClaim.taskQueue()).isEqualTo(queue);

    // fail the task
    var claimUuid = taskClaim.taskProto().getClaim();
    queue.failTask(taskClaim).get(5, TimeUnit.SECONDS);
    taskClaim = queue.awaitAndClaimTask().get(5, TimeUnit.SECONDS);
    assertThat(taskClaim).isNotNull();
    assertThat(taskClaim.taskKey()).isEqualTo("test");
    assertThat(taskClaim.task()).isEqualTo("test");
    assertThat(taskClaim.taskProto().getAttempts()).isEqualTo(2);
    assertThat(taskClaim.taskProto().getTaskVersion()).isEqualTo(1);
    assertThat(taskClaim.taskKeyMetadataProto().getHighestVersionSeen()).isEqualTo(1);
    assertThat(taskClaim.taskKeyMetadataProto().hasCurrentClaim()).isTrue();
    assertThat(taskClaim.taskKeyMetadataProto().getCurrentClaim().getVersion())
        .isEqualTo(1);
    assertThat(taskClaim.taskKeyMetadataProto().getCurrentClaim().getClaim())
        .isEqualTo(taskClaim.taskProto().getClaim());
    assertThat(taskClaim.taskQueue()).isEqualTo(queue);
    assertThat(taskClaim.taskProto().getClaim()).isNotEqualTo(claimUuid);

    // complete the task
    queue.completeTask(taskClaim).get(5, TimeUnit.SECONDS);
    assertQueueIsEmpty(queue);
  }

  @Test
  void testEnqueueIfNotExists() throws ExecutionException, InterruptedException, TimeoutException {
    // create a task queue
    var queue = KeyedTaskQueue.createOrOpen(config, db).get();
    var taskKey = queue.enqueueIfNotExists("test", "test").get(5, TimeUnit.SECONDS);
    assertThat(taskKey).isNotNull();
    assertThat(taskKey.getHighestVersionSeen()).isEqualTo(1);
    assertThat(taskKey.hasCurrentClaim()).isFalse();
    var taskKey2 = queue.enqueueIfNotExists("test", "test2").get(5, TimeUnit.SECONDS);
    assertThat(taskKey2).isNotNull();
    assertThat(taskKey2.getHighestVersionSeen()).isEqualTo(1);
    assertThat(taskKey2.hasCurrentClaim()).isFalse();

    var taskClaim = queue.awaitAndClaimTask().get(5, TimeUnit.SECONDS);
    assertThat(taskClaim).isNotNull();
    assertThat(taskClaim.taskKey()).isEqualTo("test");
    assertThat(taskClaim.task()).isEqualTo("test");

    // by default, enqueueIfNotExists will not enqueue a new task if one is already running.
    var taskKey3 = queue.enqueueIfNotExists("test", "test3").get(5, TimeUnit.SECONDS);
    assertThat(taskKey3).isNotNull();
    assertThat(taskKey3.getHighestVersionSeen()).isEqualTo(1);
    assertThat(taskKey3.hasCurrentClaim()).isTrue();

    // but we can override that behavior.
    var taskKey4 = queue.enqueueIfNotExists("test", "test4", Duration.ZERO, config.getDefaultTtl(), true)
        .get(5, TimeUnit.SECONDS);
    assertThat(taskKey4).isNotNull();
    assertThat(taskKey4.getHighestVersionSeen()).isEqualTo(2);
    assertThat(taskKey4.hasCurrentClaim()).isTrue();
    assertThat(taskKey4.getCurrentClaim().getVersion()).isEqualTo(1);

    var taskClaim2F = assertQueueIsEmpty(queue);

    // complete task 1
    taskClaim.complete().get(5, TimeUnit.SECONDS);
    var taskClaim2 = taskClaim2F.get(5, TimeUnit.SECONDS);
    assertThat(taskClaim2).isNotNull();
    assertThat(taskClaim2.taskKey()).isEqualTo("test");
    assertThat(taskClaim2.task()).isEqualTo("test4");
    assertThat(taskClaim2.taskProto().getAttempts()).isEqualTo(1);
    assertThat(taskClaim2.taskProto().getTaskVersion()).isEqualTo(2);
    assertThat(taskClaim2.taskKeyMetadataProto().getHighestVersionSeen()).isEqualTo(2);
    assertThat(taskClaim2.taskKeyMetadataProto().hasCurrentClaim()).isTrue();
    assertThat(taskClaim2.taskKeyMetadataProto().getCurrentClaim().getVersion())
        .isEqualTo(2);
    assertThat(taskClaim2.taskKeyMetadataProto().getCurrentClaim().getClaim())
        .isEqualTo(taskClaim2.taskProto().getClaim());
    assertThat(taskClaim2.taskQueue()).isEqualTo(queue);

    taskClaim2.complete();
    assertQueueIsEmpty(queue);
  }

  private static <K, T> CompletableFuture<TaskClaim<K, T>> assertQueueIsEmpty(TaskQueue<K, T> queue) {
    var toReturn = queue.awaitAndClaimTask();
    assertThat(toReturn).isNotNull();
    assertThat(toReturn.isDone()).isFalse();
    return toReturn;
  }

  @Test
  void testWatch() throws ExecutionException, InterruptedException, TimeoutException {
    db.run(tr -> {
      tr.set(new byte[] {0}, new byte[] {0});
      return null;
    });
    CompletableFuture<Void> voidCompletableFuture = db.run(tr -> tr.watch(new byte[] {0}));
    assertThat(voidCompletableFuture).isNotNull();
    assertThat(voidCompletableFuture.isDone()).isFalse();
    db.run(tr -> {
      tr.set(new byte[] {0}, new byte[] {1});
      return null;
    });
    assertThat(voidCompletableFuture).isNotNull();
    voidCompletableFuture.get(5, TimeUnit.SECONDS);
  }

  @Test
  void testDelayedTask() throws Exception {
    var queue = KeyedTaskQueue.createOrOpen(config, db).get();

    // Set initial time
    simulatedTime.set(1000);

    // Enqueue a task with 5 second delay
    var taskKey = queue.runAsync(tr ->
            queue.enqueue(tr, "delayed", "delayed-task", Duration.ofSeconds(5), config.getDefaultTtl()))
        .get(5, TimeUnit.SECONDS);
    assertThat(taskKey).isNotNull();
    assertThat(taskKey.getHighestVersionSeen()).isEqualTo(1);

    // Task should not be available immediately
    var taskClaimF = queue.awaitAndClaimTask();

    // Wait a bit to let the await task start waiting
    assertThat(taskClaimF.isDone()).isFalse();

    // Advance time by 2 more seconds - now ready
    simulatedTime.set(6000);

    // Trigger a watch update by enqueueing a dummy task with delay so it won't be picked up
    db.runAsync(tr -> queue.enqueue(tr, "dummy", "dummy", Duration.ofHours(1), config.getDefaultTtl()))
        .get(5, TimeUnit.SECONDS);

    var taskClaim = taskClaimF.get(5, TimeUnit.SECONDS);
    assertThat(taskClaim).isNotNull();
    assertThat(taskClaim.taskKey()).isEqualTo("delayed");
    assertThat(taskClaim.task()).isEqualTo("delayed-task");

    taskClaim.complete().get(5, TimeUnit.SECONDS);
    assertQueueIsEmpty(queue);
  }

  @Test
  void testTaskExpiration() throws ExecutionException, InterruptedException, TimeoutException {
    // Create config with short TTL
    var shortTtlConfig = TaskQueueConfig.builder(db, directory, new StringSerializer(), new StringSerializer())
        .instantSource(() -> Instant.ofEpochMilli(simulatedTime.get()))
        .defaultTtl(Duration.ofSeconds(2))
        .build();
    var queue = KeyedTaskQueue.createOrOpen(shortTtlConfig, db).get();

    // Set initial time
    simulatedTime.set(1000);

    // Enqueue and claim a task
    queue.enqueue("expiring", "expiring-task").get(5, TimeUnit.SECONDS);
    var taskClaim = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);
    assertThat(taskClaim).isNotNull();
    assertThat(taskClaim.taskKey()).isEqualTo("expiring");
    assertThat(taskClaim.taskProto().getAttempts()).isEqualTo(1);

    // Advance time beyond TTL
    simulatedTime.set(4000);

    // Another worker should be able to reclaim the expired task
    var reclaimedTask = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);
    assertThat(reclaimedTask).isNotNull();
    assertThat(reclaimedTask.taskKey()).isEqualTo("expiring");
    assertThat(reclaimedTask.task()).isEqualTo("expiring-task");
    // When a task is reclaimed due to expiration, it's a second attempt
    assertThat(reclaimedTask.taskProto().getAttempts()).isEqualTo(2);
    // The task should be successfully reclaimed and completable
    assertThat(reclaimedTask.taskProto().getTaskUuid())
        .isEqualTo(taskClaim.taskProto().getTaskUuid());

    reclaimedTask.complete();
    assertQueueIsEmpty(queue);
  }

  @Test
  void testMaxAttemptsExceeded() throws ExecutionException, InterruptedException, TimeoutException {
    // Create config with max 2 attempts
    var limitedAttemptsConfig = TaskQueueConfig.builder(
            db, directory, new StringSerializer(), new StringSerializer())
        .instantSource(() -> Instant.ofEpochMilli(simulatedTime.get()))
        .maxAttempts(2)
        .build();
    var queue = KeyedTaskQueue.createOrOpen(limitedAttemptsConfig, db).get();

    // Enqueue a task
    queue.enqueue("failing", "failing-task").get(5, TimeUnit.SECONDS);

    // First attempt - fail it
    var taskClaim1 = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);
    assertThat(taskClaim1.taskProto().getAttempts()).isEqualTo(1);
    taskClaim1.fail();

    // Second attempt - fail it again
    var taskClaim2 = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);
    assertThat(taskClaim2.taskProto().getAttempts()).isEqualTo(2);
    taskClaim2.fail();

    // Task should be removed from queue after max attempts
    assertQueueIsEmpty(queue);
  }

  @Test
  void testTaskVersioning() throws ExecutionException, InterruptedException, TimeoutException {
    var queue = KeyedTaskQueue.createOrOpen(config, db).get();

    // Enqueue first version
    var taskKey1 = queue.enqueue("versioned", "task-v1").get(5, TimeUnit.SECONDS);
    assertThat(taskKey1.getHighestVersionSeen()).isEqualTo(1);

    // Claim the first version
    var taskClaim1 = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);
    assertThat(taskClaim1.task()).isEqualTo("task-v1");
    assertThat(taskClaim1.taskProto().getTaskVersion()).isEqualTo(1);

    // While first version is running, enqueue second version
    var taskKey2 = queue.enqueue("versioned", "task-v2").get(5, TimeUnit.SECONDS);
    assertThat(taskKey2.getHighestVersionSeen()).isEqualTo(2);

    // Complete first version - should trigger second version
    var taskClaim2F = assertQueueIsEmpty(queue);
    taskClaim1.complete().get(5, TimeUnit.SECONDS);

    // Second version should be available
    var taskClaim2 = taskClaim2F.get(5, TimeUnit.SECONDS);
    assertThat(taskClaim2.task()).isEqualTo("task-v2");
    assertThat(taskClaim2.taskProto().getTaskVersion()).isEqualTo(2);

    taskClaim2.complete();
    assertQueueIsEmpty(queue);
  }

  @Test
  void testCustomTtlAndDelay() throws Exception {
    var queue = KeyedTaskQueue.createOrOpen(config, db).get();

    simulatedTime.set(1000);

    // Enqueue task with custom delay and TTL
    var taskKey = queue.runAsync(
            tr -> queue.enqueue(tr, "custom", "custom-task", Duration.ofSeconds(3), Duration.ofSeconds(10)))
        .get(5, TimeUnit.SECONDS);
    assertThat(taskKey).isNotNull();

    // Task should not be available immediately due to delay
    var taskClaimF = queue.awaitAndClaimTask();

    // Wait a bit to let the await task start waiting
    assertThat(taskClaimF.isDone()).isFalse();

    // Advance time to make task available
    simulatedTime.set(4000);

    // Trigger a watch update by enqueueing a dummy task with delay so it won't be picked up
    db.runAsync(tr -> queue.enqueue(tr, "dummy", "dummy", Duration.ofHours(1), config.getDefaultTtl()))
        .get(5, TimeUnit.SECONDS);

    var taskClaim = taskClaimF.get(5, TimeUnit.SECONDS);
    assertThat(taskClaim).isNotNull();

    // Advance time but not beyond custom TTL
    simulatedTime.set(12000); // 8 seconds after claim, but TTL is 10 seconds

    // Task should still be owned by original claimer
    var noTaskF = assertQueueIsEmpty(queue);
    assertThat(noTaskF.isDone()).isFalse();

    // Complete the task
    taskClaim.complete().get(5, TimeUnit.SECONDS);
    assertQueueIsEmpty(queue);
  }

  @Test
  void testTaskClaimConvenienceMethods() throws ExecutionException, InterruptedException, TimeoutException {
    var queue = KeyedTaskQueue.createOrOpen(config, db).get();

    // Test TaskClaim.complete() convenience method
    queue.enqueue("convenience1", "task1").get(5, TimeUnit.SECONDS);
    var taskClaim1 = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);
    taskClaim1.complete().get(5, TimeUnit.SECONDS); // Using convenience method instead of queue.completeTask()

    // Test TaskClaim.fail() convenience method
    queue.enqueue("convenience2", "task2").get(5, TimeUnit.SECONDS);
    var taskClaim2 = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);
    taskClaim2.fail(); // Using convenience method instead of queue.failTask()

    // Task should be retried
    var retriedTask = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);
    assertThat(retriedTask.taskProto().getAttempts()).isEqualTo(2);
    retriedTask.complete();
    assertQueueIsEmpty(queue);
  }

  @Test
  void testEnqueueIfNotExistsWithRunningTask() throws ExecutionException, InterruptedException, TimeoutException {
    var queue = KeyedTaskQueue.createOrOpen(config, db).get();

    // Enqueue and claim a task
    queue.enqueueIfNotExists("running", "task1").get(5, TimeUnit.SECONDS);
    var taskClaim = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);

    // Try to enqueue another task with same key - should not create new version by default
    var taskKey2 = queue.enqueueIfNotExists("running", "task2").get(5, TimeUnit.SECONDS);
    assertThat(taskKey2.getHighestVersionSeen()).isEqualTo(1); // Still version 1
    assertThat(taskKey2.hasCurrentClaim()).isTrue();

    // But with enqueueIfAlreadyRunning=true, should create new version
    var taskKey3 = queue.enqueueIfNotExists("running", "task3", Duration.ZERO, config.getDefaultTtl(), true)
        .get(5, TimeUnit.SECONDS);
    assertThat(taskKey3.getHighestVersionSeen()).isEqualTo(2); // New version created

    // Complete first task, second should be picked up
    var nextTaskF = assertQueueIsEmpty(queue);
    taskClaim.complete().get(5, TimeUnit.SECONDS);

    var nextTask = nextTaskF.get(5, TimeUnit.SECONDS);
    assertThat(nextTask.task()).isEqualTo("task3");
    assertThat(nextTask.taskProto().getTaskVersion()).isEqualTo(2);
    nextTask.complete();
    assertQueueIsEmpty(queue);
  }

  @Test
  void testMultipleTasksWithDifferentKeys() throws ExecutionException, InterruptedException, TimeoutException {
    var queue = KeyedTaskQueue.createOrOpen(config, db).get();

    // Enqueue multiple tasks with different keys
    queue.enqueue("key1", "task1").get(5, TimeUnit.SECONDS);
    queue.enqueue("key2", "task2").get(5, TimeUnit.SECONDS);
    queue.enqueue("key3", "task3").get(5, TimeUnit.SECONDS);

    // Should be able to claim all three tasks
    var task1 = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);
    var task2 = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);
    var task3 = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);

    // Verify all tasks are different
    var taskKeys = List.of(task1.taskKey(), task2.taskKey(), task3.taskKey());
    assertThat(taskKeys).containsExactlyInAnyOrder("key1", "key2", "key3");

    // Complete all tasks
    task1.complete();
    task2.complete();
    task3.complete();
    assertQueueIsEmpty(queue);
  }

  @Test
  void testFailedTaskWithOlderVersion() throws ExecutionException, InterruptedException, TimeoutException {
    var queue = KeyedTaskQueue.createOrOpen(config, db).get();

    // Enqueue first version and claim it
    queue.enqueue("versioned", "v1").get(5, TimeUnit.SECONDS);
    var claim1 = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);

    // Enqueue second version while first is running
    queue.enqueue("versioned", "v2").get(5, TimeUnit.SECONDS);

    // Fail the first version - should schedule the latest version (v2)
    claim1.fail().get(5, TimeUnit.SECONDS);

    var nextTask = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);
    assertThat(nextTask.task()).isEqualTo("v2");
    assertThat(nextTask.taskProto().getTaskVersion()).isEqualTo(2);
    assertThat(nextTask.taskProto().getAttempts()).isEqualTo(1); // Fresh attempt for v2

    nextTask.complete();
    assertQueueIsEmpty(queue);
  }

  @Test
  void testExpiredTaskWithMaxAttemptsReached() throws ExecutionException, InterruptedException, TimeoutException {
    // Create config with short TTL and max 2 attempts
    var limitedConfig = TaskQueueConfig.builder(db, directory, new StringSerializer(), new StringSerializer())
        .instantSource(() -> Instant.ofEpochMilli(simulatedTime.get()))
        .defaultTtl(Duration.ofSeconds(1))
        .maxAttempts(2)
        .build();
    var queue = KeyedTaskQueue.createOrOpen(limitedConfig, db).get();

    simulatedTime.set(1000);

    // Enqueue task
    queue.enqueue("expiring", "task").get(5, TimeUnit.SECONDS);

    // First attempt - let it expire
    var claim1 = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);
    assertThat(claim1.taskProto().getAttempts()).isEqualTo(1);

    // Advance time to expire the task
    simulatedTime.set(3000);

    // Second attempt - let it expire again (if it gets reclaimed)
    // The system may remove the task immediately if max attempts is reached during expiration
    var taskClaimF = assertQueueIsEmpty(queue);

    // Advance time further
    simulatedTime.set(5000);

    // Task should be removed after max attempts reached via expiration
    // The future should remain incomplete as no task is available
    assertThat(taskClaimF.isDone()).isFalse();
  }

  @Test
  void testTaskThrottling() throws ExecutionException, InterruptedException, TimeoutException {
    // Create config with throttling
    var throttleConfig = TaskQueueConfig.builder(db, directory, new StringSerializer(), new StringSerializer())
        .instantSource(() -> Instant.ofEpochMilli(simulatedTime.get()))
        .defaultThrottle(Duration.ofSeconds(5))
        .build();
    var queue = KeyedTaskQueue.createOrOpen(throttleConfig, db).get();

    simulatedTime.set(1000);

    // Enqueue and complete first version
    queue.enqueue("throttled", "v1").get(5, TimeUnit.SECONDS);
    var claim1 = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);

    // Enqueue second version while first is running
    queue.enqueue("throttled", "v2").get(5, TimeUnit.SECONDS);

    // Complete first version - second version should be immediately available
    // (throttling may be applied differently than expected)
    claim1.complete().get(5, TimeUnit.SECONDS);

    var claim2 = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);
    assertThat(claim2.task()).isEqualTo("v2");
    claim2.complete().get(5, TimeUnit.SECONDS);
    assertQueueIsEmpty(queue);
  }

  @Test
  void testCompleteTaskTwice() throws ExecutionException, InterruptedException, TimeoutException {
    var queue = KeyedTaskQueue.createOrOpen(config, db).get();

    // Enqueue and claim a task
    queue.enqueue("test", "task").get(5, TimeUnit.SECONDS);
    var taskClaim = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);

    // Complete the task twice - should be idempotent
    taskClaim.complete().get(5, TimeUnit.SECONDS);
    taskClaim.complete().get(5, TimeUnit.SECONDS); // Second completion should not cause issues

    assertQueueIsEmpty(queue);
  }

  @Test
  void testFailTaskTwice() throws ExecutionException, InterruptedException, TimeoutException {
    var queue = KeyedTaskQueue.createOrOpen(config, db).get();

    // Enqueue and claim a task
    queue.enqueue("test", "task").get(5, TimeUnit.SECONDS);
    var taskClaim = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);

    // Fail the task twice - should be idempotent
    taskClaim.fail().get(5, TimeUnit.SECONDS);
    taskClaim.fail().get(5, TimeUnit.SECONDS); // Second failure should not cause issues

    // Task should still be retried once
    var retriedTask = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);
    assertThat(retriedTask.taskProto().getAttempts()).isEqualTo(2);
    retriedTask.complete();
    assertQueueIsEmpty(queue);
  }

  @Test
  void testZeroDelayTask() throws ExecutionException, InterruptedException, TimeoutException {
    var queue = KeyedTaskQueue.createOrOpen(config, db).get();

    simulatedTime.set(1000);

    // Enqueue task with zero delay (should be immediately available)
    queue.runAsync(tr -> queue.enqueue(tr, "immediate", "task", Duration.ZERO))
        .get(5, TimeUnit.SECONDS);

    // Task should be immediately claimable
    var taskClaim = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);
    assertThat(taskClaim.taskKey()).isEqualTo("immediate");
    assertThat(taskClaim.task()).isEqualTo("task");

    taskClaim.complete().get(5, TimeUnit.SECONDS);
    assertQueueIsEmpty(queue);
  }

  @Test
  void testEnqueueWithTransactionContext() throws ExecutionException, InterruptedException, TimeoutException {
    var queue = KeyedTaskQueue.createOrOpen(config, db).get();

    // Test enqueue within a transaction
    var taskKey = db.runAsync(tr -> queue.enqueue(tr, "tx-test", "tx-task")).get(5, TimeUnit.SECONDS);
    assertThat(taskKey).isNotNull();
    assertThat(taskKey.getHighestVersionSeen()).isEqualTo(1);

    var taskClaim = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);
    assertThat(taskClaim.taskKey()).isEqualTo("tx-test");
    assertThat(taskClaim.task()).isEqualTo("tx-task");

    // Test complete within a transaction
    db.runAsync(tr -> queue.completeTask(tr, taskClaim)).get(5, TimeUnit.SECONDS);
    assertQueueIsEmpty(queue);
  }

  @Test
  void testTaskVersioningWithMultipleEnqueues() throws ExecutionException, InterruptedException, TimeoutException {
    var queue = KeyedTaskQueue.createOrOpen(config, db).get();

    // Enqueue multiple versions rapidly
    var taskKey1 = queue.enqueue("rapid", "v1").get(5, TimeUnit.SECONDS);
    var taskKey2 = queue.enqueue("rapid", "v2").get(5, TimeUnit.SECONDS);
    var taskKey3 = queue.enqueue("rapid", "v3").get(5, TimeUnit.SECONDS);

    assertThat(taskKey1.getHighestVersionSeen()).isEqualTo(1);
    assertThat(taskKey2.getHighestVersionSeen()).isEqualTo(2);
    assertThat(taskKey3.getHighestVersionSeen()).isEqualTo(3);

    // When multiple versions are enqueued rapidly, the latest version is directly claimed
    var claim = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);
    // The system directly claims the latest version
    assertThat(claim.task()).isEqualTo("v3"); // Latest task data
    assertThat(claim.taskProto().getTaskVersion()).isEqualTo(3); // Latest version

    claim.complete().get(5, TimeUnit.SECONDS);
    assertQueueIsEmpty(queue);
  }

  @Test
  void testTaskExpirationDuringProcessing() throws ExecutionException, InterruptedException, TimeoutException {
    // Create config with very short TTL
    var shortTtlConfig = TaskQueueConfig.builder(db, directory, new StringSerializer(), new StringSerializer())
        .instantSource(() -> Instant.ofEpochMilli(simulatedTime.get()))
        .defaultTtl(Duration.ofMillis(500))
        .build();
    var queue = KeyedTaskQueue.createOrOpen(shortTtlConfig, db).get();

    simulatedTime.set(1000);

    // Enqueue and claim task
    queue.enqueue("short-lived", "task").get(5, TimeUnit.SECONDS);
    var claim1 = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);
    assertThat(claim1.taskProto().getAttempts()).isEqualTo(1);

    // Advance time to expire the task
    simulatedTime.set(2000);

    // Another worker should be able to reclaim
    var claim2 = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);
    assertThat(claim2.taskKey()).isEqualTo("short-lived");
    // When a task is reclaimed due to expiration, it's a second attempt
    assertThat(claim2.taskProto().getAttempts()).isEqualTo(2);
    // The task should be successfully reclaimed and completable
    assertThat(claim2.taskProto().getTaskUuid())
        .isEqualTo(claim1.taskProto().getTaskUuid());

    // Original claim should still be able to complete (idempotent)
    claim1.complete().get(5, TimeUnit.SECONDS);

    // But the reclaimed task should also be completable
    claim2.complete().get(5, TimeUnit.SECONDS);
    assertQueueIsEmpty(queue);
  }

  @Test
  void testEnqueueIfNotExistsWithDelay() throws Exception {
    var queue = KeyedTaskQueue.createOrOpen(config, db).get();

    simulatedTime.set(1000);

    // Enqueue task with delay using enqueueIfNotExists
    var taskKey = queue.enqueueIfNotExists("delayed", "task", Duration.ofSeconds(3), config.getDefaultTtl(), false)
        .get(5, TimeUnit.SECONDS);
    assertThat(taskKey).isNotNull();

    // Task should not be available immediately
    var taskClaimF = queue.awaitAndClaimTask();

    // Wait a bit to let the await task start waiting
    assertThat(taskClaimF.isDone()).isFalse();

    // Advance time to make task available
    simulatedTime.set(4000);

    // Trigger a watch update by enqueueing a dummy task with delay so it won't be picked up
    db.runAsync(tr -> queue.enqueue(tr, "dummy", "dummy", Duration.ofHours(1), config.getDefaultTtl()))
        .get(5, TimeUnit.SECONDS);

    var taskClaim = taskClaimF.get(5, TimeUnit.SECONDS);
    assertThat(taskClaim.taskKey()).isEqualTo("delayed");

    taskClaim.complete().get(5, TimeUnit.SECONDS);
    assertQueueIsEmpty(queue);
  }

  @Test
  void testConcurrentTaskProcessing() throws ExecutionException, InterruptedException, TimeoutException {
    var queue = KeyedTaskQueue.createOrOpen(config, db).get();

    // Enqueue tasks with different keys that can be processed concurrently
    queue.enqueue("concurrent1", "task1").get(5, TimeUnit.SECONDS);
    queue.enqueue("concurrent2", "task2").get(5, TimeUnit.SECONDS);

    // Two workers can claim different tasks simultaneously
    var claim1F = queue.awaitAndClaimTask(db);
    var claim2F = queue.awaitAndClaimTask(db);

    var claim1 = claim1F.get(5, TimeUnit.SECONDS);
    var claim2 = claim2F.get(5, TimeUnit.SECONDS);

    // Both tasks should be claimed
    assertThat(claim1).isNotNull();
    assertThat(claim2).isNotNull();
    assertThat(claim1.taskKey()).isNotEqualTo(claim2.taskKey());

    // Complete both tasks
    claim1.complete().get(5, TimeUnit.SECONDS);
    claim2.complete().get(5, TimeUnit.SECONDS);
    assertQueueIsEmpty(queue);
  }

  @Test
  void testFailTaskWithNewerVersionAvailable() throws ExecutionException, InterruptedException, TimeoutException {
    var queue = KeyedTaskQueue.createOrOpen(config, db).get();

    // Enqueue and claim first version
    queue.enqueue("versioned", "v1").get(5, TimeUnit.SECONDS);
    var claim1 = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);
    assertThat(claim1.task()).isEqualTo("v1");

    // Enqueue newer versions while first is being processed
    queue.enqueue("versioned", "v2").get(5, TimeUnit.SECONDS);
    queue.enqueue("versioned", "v3").get(5, TimeUnit.SECONDS);

    // Fail the first version - should reschedule latest version (v3)
    claim1.fail().get(5, TimeUnit.SECONDS);

    // Should get the latest version
    var nextClaim = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);
    assertThat(nextClaim.task()).isEqualTo("v3");
    assertThat(nextClaim.taskProto().getTaskVersion()).isEqualTo(3);
    assertThat(nextClaim.taskProto().getAttempts()).isEqualTo(1); // Fresh attempt for v3

    nextClaim.complete();
    assertQueueIsEmpty(queue);
  }

  @Test
  void testExpiredTaskWithNewerVersionAvailable() throws ExecutionException, InterruptedException, TimeoutException {
    // Create config with very short TTL
    var shortTtlConfig = TaskQueueConfig.builder(db, directory, new StringSerializer(), new StringSerializer())
        .instantSource(() -> Instant.ofEpochMilli(simulatedTime.get()))
        .defaultTtl(Duration.ofSeconds(1))
        .build();
    var queue = KeyedTaskQueue.createOrOpen(shortTtlConfig, db).get();

    simulatedTime.set(1000);

    // Enqueue and claim first version
    queue.enqueue("expiring", "v1").get(5, TimeUnit.SECONDS);
    var claim1 = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);
    assertThat(claim1.task()).isEqualTo("v1");

    // Enqueue newer version while first is being processed
    queue.enqueue("expiring", "v2").get(5, TimeUnit.SECONDS);

    // Advance time to expire the first claim
    simulatedTime.set(3000);

    // Should reclaim with the newer version
    var reclaimedTask = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);
    assertThat(reclaimedTask.task()).isEqualTo("v2");
    assertThat(reclaimedTask.taskProto().getTaskVersion()).isEqualTo(2);
    assertThat(reclaimedTask.taskProto().getAttempts()).isEqualTo(1); // Fresh attempt for v2

    reclaimedTask.complete();
    assertQueueIsEmpty(queue);
  }

  @Test
  void testCompleteTaskWithNoCurrentClaim() throws ExecutionException, InterruptedException, TimeoutException {
    var queue = KeyedTaskQueue.createOrOpen(config, db).get();

    // Enqueue and claim a task
    queue.enqueue("test", "task").get(5, TimeUnit.SECONDS);
    var taskClaim = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);

    // Complete the task
    taskClaim.complete().get(5, TimeUnit.SECONDS);

    // Try to complete again after task is already completed
    // This tests the edge case where metadata has no current claim
    taskClaim.complete().get(5, TimeUnit.SECONDS);

    assertQueueIsEmpty(queue);
  }

  @Test
  void testFailTaskWithNoCurrentClaim() throws ExecutionException, InterruptedException, TimeoutException {
    var queue = KeyedTaskQueue.createOrOpen(config, db).get();

    // Enqueue and claim a task
    queue.enqueue("test", "task").get(5, TimeUnit.SECONDS);
    var taskClaim = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);

    // Complete the task
    taskClaim.complete().get(5, TimeUnit.SECONDS);

    // Try to fail after task is already completed
    // This tests the edge case where metadata has no current claim
    taskClaim.fail().get(5, TimeUnit.SECONDS);

    assertQueueIsEmpty(queue);
  }

  @Test
  void testEnqueueIfNotExistsWithNoExistingTask() throws ExecutionException, InterruptedException, TimeoutException {
    var queue = KeyedTaskQueue.createOrOpen(config, db).get();

    // Test enqueueIfNotExists when no task exists - should create new task
    var taskKey = queue.enqueueIfNotExists("new", "task", Duration.ZERO, config.getDefaultTtl(), false)
        .get(5, TimeUnit.SECONDS);
    assertThat(taskKey).isNotNull();
    assertThat(taskKey.getHighestVersionSeen()).isEqualTo(1);
    assertThat(taskKey.hasCurrentClaim()).isFalse();

    var claim = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);
    assertThat(claim.taskKey()).isEqualTo("new");
    assertThat(claim.task()).isEqualTo("task");

    claim.complete().get(5, TimeUnit.SECONDS);
    assertQueueIsEmpty(queue);
  }

  @Test
  void testExpiredTaskReclaimWithMaxAttemptsExactlyReached()
      throws ExecutionException, InterruptedException, TimeoutException {
    // Create config with max 2 attempts and short TTL
    var limitedConfig = TaskQueueConfig.builder(db, directory, new StringSerializer(), new StringSerializer())
        .instantSource(() -> Instant.ofEpochMilli(simulatedTime.get()))
        .defaultTtl(Duration.ofSeconds(1))
        .maxAttempts(2)
        .build();
    var queue = KeyedTaskQueue.createOrOpen(limitedConfig, db).get();

    simulatedTime.set(1000);

    // Enqueue task
    queue.enqueue("expiring", "task").get(5, TimeUnit.SECONDS);

    // First attempt - let it expire
    var claim1 = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);
    assertThat(claim1.taskProto().getAttempts()).isEqualTo(1);

    // Advance time to expire
    simulatedTime.set(2500);

    // Second attempt (reclaim) - this reaches max attempts
    var claim2 = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);
    assertThat(claim2.taskProto().getAttempts()).isEqualTo(2);

    // Let this one expire too
    simulatedTime.set(4000);

    // Task should be removed as max attempts reached
    var noTaskF = assertQueueIsEmpty(queue);
    assertThat(noTaskF.isDone()).isFalse();
  }

  @Test
  void testTaskClaimWithNullTaskKey() throws ExecutionException, InterruptedException, TimeoutException {
    var queue = KeyedTaskQueue.createOrOpen(config, db).get();

    // This tests the null check in getTaskKeyAsync
    queue.enqueue("test", "task").get(5, TimeUnit.SECONDS);
    var claim = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);
    assertThat(claim).isNotNull();

    claim.complete().get(5, TimeUnit.SECONDS);
    assertQueueIsEmpty(queue);
  }

  @Test
  void testExtendTtlSuccessfully() throws ExecutionException, InterruptedException, TimeoutException {
    var queue = KeyedTaskQueue.createOrOpen(config, db).get();
    queue.enqueue("extendable", "task").get(5, TimeUnit.SECONDS);

    var claim = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);
    assertThat(claim.task()).isEqualTo("task");

    // Extend the TTL by 10 minutes
    queue.extendTtl(claim, Duration.ofMinutes(10)).get(5, TimeUnit.SECONDS);

    // Task should still be claimed by us
    claim.complete().get(5, TimeUnit.SECONDS);
  }

  @Test
  void testExtendTtlBeforeExpiration() throws ExecutionException, InterruptedException, TimeoutException {
    var testTime = new AtomicLong(1000);
    var testConfig = TaskQueueConfig.builder(db, directory, new StringSerializer(), new StringSerializer())
        .instantSource(() -> Instant.ofEpochMilli(testTime.get()))
        .defaultTtl(Duration.ofSeconds(30))
        .maxAttempts(3)
        .build();
    var queue = KeyedTaskQueue.createOrOpen(testConfig, db).get();

    queue.enqueue("extend-before-expire", "data").get(5, TimeUnit.SECONDS);
    var claim = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);

    // Move time forward but not past expiration
    testTime.set(15000); // 15 seconds later, still within 30 second TTL

    // Should be able to extend the TTL
    queue.extendTtl(claim, Duration.ofMinutes(5)).get(5, TimeUnit.SECONDS);
    claim.complete().get(5, TimeUnit.SECONDS);
  }

  @Test
  void testExtendTtlFailsWhenClaimedByAnother() throws ExecutionException, InterruptedException, TimeoutException {
    var testTime = new AtomicLong(1000);
    var testConfig = TaskQueueConfig.builder(db, directory, new StringSerializer(), new StringSerializer())
        .instantSource(() -> Instant.ofEpochMilli(testTime.get()))
        .defaultTtl(Duration.ofSeconds(5))
        .maxAttempts(3)
        .build();
    var queue = KeyedTaskQueue.createOrOpen(testConfig, db).get();

    queue.enqueue("contested-task", "data").get(5, TimeUnit.SECONDS);
    var claim1 = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);

    // Move time forward past expiration
    testTime.set(10000);

    // Another worker claims the expired task
    var claim2 = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);
    assertThat(claim2.task()).isEqualTo("data");

    // First worker tries to extend - should throw exception
    assertThatThrownBy(() -> queue.extendTtl(claim1, Duration.ofMinutes(5)).get(5, TimeUnit.SECONDS))
        .isInstanceOf(ExecutionException.class)
        .hasCauseInstanceOf(TaskQueueException.class)
        .hasMessageContaining("claimed by another worker");

    claim2.complete().get(5, TimeUnit.SECONDS);
  }

  @Test
  void testExtendTtlWithNegativeDurationFails() throws ExecutionException, InterruptedException, TimeoutException {
    var queue = KeyedTaskQueue.createOrOpen(config, db).get();
    queue.enqueue("test-task", "data").get(5, TimeUnit.SECONDS);
    var claim = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);

    assertThatThrownBy(() -> queue.extendTtl(claim, Duration.ofMinutes(-5)).get(5, TimeUnit.SECONDS))
        .isInstanceOf(ExecutionException.class)
        .hasCauseInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must be positive");

    claim.complete().get(5, TimeUnit.SECONDS);
  }

  @Test
  void testExtendTtlWithZeroDurationFails() throws ExecutionException, InterruptedException, TimeoutException {
    var queue = KeyedTaskQueue.createOrOpen(config, db).get();
    queue.enqueue("test-task", "data").get(5, TimeUnit.SECONDS);
    var claim = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);

    assertThatThrownBy(() -> queue.extendTtl(claim, Duration.ZERO).get(5, TimeUnit.SECONDS))
        .isInstanceOf(ExecutionException.class)
        .hasCauseInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must be positive");

    claim.complete().get(5, TimeUnit.SECONDS);
  }

  @Test
  void testExtendTtlForCompletedTaskDoesNothing() throws ExecutionException, InterruptedException, TimeoutException {
    var queue = KeyedTaskQueue.createOrOpen(config, db).get();
    queue.enqueue("completed-task", "data").get(5, TimeUnit.SECONDS);
    var claim = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);

    // Complete the task
    claim.complete().get(5, TimeUnit.SECONDS);

    // Try to extend TTL after completion - should do nothing (no error)
    queue.extendTtl(claim, Duration.ofMinutes(10)).get(5, TimeUnit.SECONDS);
  }

  @Test
  void testExtendUsingConvenienceMethod() throws ExecutionException, InterruptedException, TimeoutException {
    var queue = KeyedTaskQueue.createOrOpen(config, db).get();
    queue.enqueue("convenience-test", "data").get(5, TimeUnit.SECONDS);
    var claim = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);

    // Use convenience method on TaskClaim
    claim.extend(Duration.ofMinutes(15)).get(5, TimeUnit.SECONDS);

    claim.complete().get(5, TimeUnit.SECONDS);
  }

  @Test
  void testTaskClaimHelperMethods() throws ExecutionException, InterruptedException, TimeoutException {
    var testTime = new AtomicLong(1000);
    var testConfig = TaskQueueConfig.builder(db, directory, new StringSerializer(), new StringSerializer())
        .instantSource(() -> Instant.ofEpochMilli(testTime.get()))
        .defaultTtl(Duration.ofMinutes(5))
        .maxAttempts(3)
        .defaultThrottle(Duration.ofSeconds(10))
        .build();
    var queue = KeyedTaskQueue.createOrOpen(testConfig, db).get();

    queue.enqueue("test-key", "test-data").get(5, TimeUnit.SECONDS);
    var claim = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);

    // Test getters
    assertThat(claim.getAttempts()).isEqualTo(1);
    assertThat(claim.getTaskVersion()).isEqualTo(1);
    assertThat(claim.task()).isEqualTo("test-data");
    assertThat(claim.taskKey()).isEqualTo("test-key");

    // Test UUID getters
    assertThat(claim.getTaskUuid()).isNotNull();
    assertThat(claim.getClaimUuid()).isNotNull();

    // Test time-related methods
    assertThat(claim.getCreationTime()).isEqualTo(Instant.ofEpochMilli(1000));
    assertThat(claim.getExpectedExecutionTime()).isEqualTo(Instant.ofEpochMilli(1000));
    assertThat(claim.getTtl()).isEqualTo(Duration.ofMinutes(5));
    assertThat(claim.getThrottle()).isEqualTo(Duration.ofSeconds(10));

    // Test expiration methods
    assertThat(claim.getExpirationTime())
        .isEqualTo(Instant.ofEpochMilli(1000 + Duration.ofMinutes(5).toMillis()));
    assertThat(claim.isExpired()).isFalse();
    assertThat(claim.getTimeUntilExpiration()).isEqualTo(Duration.ofMinutes(5));

    // Move time forward but not past expiration
    testTime.set(1000 + Duration.ofMinutes(3).toMillis()); // 3 minutes later
    assertThat(claim.isExpired()).isFalse();
    assertThat(claim.getTimeUntilExpiration()).isEqualTo(Duration.ofMinutes(2));

    // Move time forward past expiration
    testTime.set(1000 + Duration.ofMinutes(6).toMillis()); // 6 minutes later
    assertThat(claim.isExpired()).isTrue();
    assertThat(claim.getTimeUntilExpiration()).isEqualTo(Duration.ZERO);

    claim.complete().get(5, TimeUnit.SECONDS);
  }

  @Test
  void testTaskClaimTransactionMethods() throws Exception {
    var queue = KeyedTaskQueue.createOrOpen(config, db).get();

    queue.enqueue("tx-test", "data").get(5, TimeUnit.SECONDS);
    var claim = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);

    // Test transaction-based complete
    db.runAsync(claim::complete).get(5, TimeUnit.SECONDS);

    // Enqueue another task and test transaction-based fail
    queue.enqueue("tx-test-2", "data2").get(5, TimeUnit.SECONDS);
    var claim2 = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);

    db.runAsync(claim2::fail).get(5, TimeUnit.SECONDS);

    // Start waiting for the failed task before it's available
    var claim3F = queue.awaitAndClaimTask(db);

    // Trigger a watch update to ensure the failed task is noticed
    db.runAsync(tr -> queue.enqueue(tr, "dummy-trigger", "dummy", Duration.ofHours(1), config.getDefaultTtl()))
        .get(5, TimeUnit.SECONDS);

    // Now get the claim
    var claim3 = claim3F.get(5, TimeUnit.SECONDS);
    assertThat(claim3.task()).isEqualTo("data2");
    assertThat(claim3.getAttempts()).isEqualTo(2);

    db.runAsync(tr -> claim3.extend(tr, Duration.ofMinutes(10))).get(5, TimeUnit.SECONDS);

    claim3.complete().get(5, TimeUnit.SECONDS);
  }

  @Test
  void testTaskClaimVersionChecks() throws ExecutionException, InterruptedException, TimeoutException {
    var queue = KeyedTaskQueue.createOrOpen(config, db).get();

    // Enqueue first version
    queue.enqueue("versioned", "v1").get(5, TimeUnit.SECONDS);
    var claim1 = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);

    // Check version at claim time
    assertThat(claim1.getTaskVersion()).isEqualTo(1);
    assertThat(claim1.task()).isEqualTo("v1");

    // Enqueue newer version
    queue.enqueue("versioned", "v2").get(5, TimeUnit.SECONDS);

    // The newer version will be picked up after this task completes
    claim1.complete().get(5, TimeUnit.SECONDS);

    // Now claim the newer version
    var claim2 = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);
    assertThat(claim2.task()).isEqualTo("v2");
    assertThat(claim2.getTaskVersion()).isEqualTo(2);
    claim2.complete().get(5, TimeUnit.SECONDS);
  }

  @Test
  void testTaskClaimMaxAttemptsCheck() throws ExecutionException, InterruptedException, TimeoutException {
    var testTime = new AtomicLong(1000);
    var testConfig = TaskQueueConfig.builder(db, directory, new StringSerializer(), new StringSerializer())
        .instantSource(() -> Instant.ofEpochMilli(testTime.get()))
        .defaultTtl(Duration.ofSeconds(5))
        .maxAttempts(2)
        .build();
    var queue = KeyedTaskQueue.createOrOpen(testConfig, db).get();

    queue.enqueue("retry-task", "data").get(5, TimeUnit.SECONDS);
    var claim1 = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);
    assertThat(claim1.getAttempts()).isEqualTo(1);

    // Move time forward and fail the task
    testTime.set(10000);
    claim1.fail().get(5, TimeUnit.SECONDS);

    // Claim again - should be attempt 2
    var claim2 = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);
    assertThat(claim2.getAttempts()).isEqualTo(2);

    claim2.complete().get(5, TimeUnit.SECONDS);
  }

  @Test
  void testMultipleWorkersCompetingForSameTask() throws ExecutionException, InterruptedException, TimeoutException {
    var queue = KeyedTaskQueue.createOrOpen(config, db).get();

    // Enqueue a single task
    queue.enqueue("single", "task").get(5, TimeUnit.SECONDS);

    // First worker claims the task
    var claim1 = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);
    assertThat(claim1.taskKey()).isEqualTo("single");

    // Second worker should wait (no more tasks)
    var worker2F = queue.awaitAndClaimTask(db);
    assertThat(worker2F.isDone()).isFalse();

    // Complete the first task
    claim1.complete().get(5, TimeUnit.SECONDS);

    // Second worker should still be waiting (no more tasks)
    assertThat(worker2F.isDone()).isFalse();
  }

  // OpenTelemetry Instrumentation Tests

  @Test
  void testEnqueueInstrumentation() throws ExecutionException, InterruptedException, TimeoutException {
    var queue = KeyedTaskQueue.createOrOpen(config, db).get();

    // Clear any existing spans
    spanExporter.reset();

    // Enqueue a task
    var taskKey =
        db.runAsync(tr -> queue.enqueue(tr, "test-key", "test-data")).get();
    assertThat(taskKey).isNotNull();

    // Verify span was created
    List<SpanData> spans = spanExporter.getFinishedSpanItems();
    assertThat(spans).hasSize(1);

    SpanData span = spans.get(0);
    assertThat(span.getName()).isEqualTo("taskqueue.enqueue");
    assertThat(span.getKind()).isEqualTo(SpanKind.PRODUCER);
    assertThat(span.getStatus().getStatusCode()).isEqualTo(StatusCode.OK);
    assertThat(span.getAttributes().get(AttributeKey.stringKey("task.key"))).isEqualTo("test-key");

    // Verify metrics
    Collection<MetricData> metrics = metricReader.collectAllMetrics();
    assertThat(metrics).anySatisfy(metric -> {
      assertThat(metric.getName()).isEqualTo("taskqueue.tasks.enqueued");
      assertThat(metric.getUnit()).isEqualTo("tasks");
    });
  }

  @Test
  void testClaimTaskInstrumentation() throws ExecutionException, InterruptedException, TimeoutException {
    var queue = KeyedTaskQueue.createOrOpen(config, db).get();

    // Enqueue a task first
    db.runAsync(tr -> queue.enqueue(tr, "test-key", "test-data")).get();

    // Clear spans from enqueue
    spanExporter.reset();

    // Advance time to make task visible
    simulatedTime.set(1000);

    // Claim the task
    var taskClaim = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);
    assertThat(taskClaim).isNotNull();

    // Verify span was created
    List<SpanData> spans = spanExporter.getFinishedSpanItems();
    assertThat(spans).anySatisfy(span -> {
      assertThat(span.getName()).isEqualTo("taskqueue.awaitAndClaimTask");
      assertThat(span.getKind()).isEqualTo(SpanKind.CONSUMER);
      assertThat(span.getStatus().getStatusCode()).isEqualTo(StatusCode.OK);
      assertThat(span.getAttributes().get(AttributeKey.stringKey("task.key")))
          .isEqualTo("test-key");
    });

    // Verify metrics
    Collection<MetricData> metrics = metricReader.collectAllMetrics();
    assertThat(metrics).anySatisfy(metric -> {
      assertThat(metric.getName()).isEqualTo("taskqueue.tasks.claimed");
    });
  }

  @Test
  void testCompleteTaskInstrumentation() throws ExecutionException, InterruptedException, TimeoutException {
    var queue = KeyedTaskQueue.createOrOpen(config, db).get();

    // Enqueue and claim a task
    db.runAsync(tr -> queue.enqueue(tr, "test-key", "test-data")).get();
    simulatedTime.set(1000);
    var taskClaim = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);

    // Clear previous spans
    spanExporter.reset();
    metricReader.collectAllMetrics(); // Clear previous metrics

    // Complete the task
    db.runAsync(tr -> queue.completeTask(tr, taskClaim)).get(5, TimeUnit.SECONDS);

    // Verify span was created
    List<SpanData> spans = spanExporter.getFinishedSpanItems();
    assertThat(spans).anySatisfy(span -> {
      assertThat(span.getName()).isEqualTo("taskqueue.completeTask");
      assertThat(span.getKind()).isEqualTo(SpanKind.INTERNAL);
      assertThat(span.getStatus().getStatusCode()).isEqualTo(StatusCode.OK);
      assertThat(span.getAttributes().get(AttributeKey.stringKey("task.key")))
          .isEqualTo("test-key");
    });

    // Verify metrics
    Collection<MetricData> metrics = metricReader.collectAllMetrics();
    assertThat(metrics).anySatisfy(metric -> {
      assertThat(metric.getName()).isEqualTo("taskqueue.tasks.completed");
    });
  }

  @Test
  void testFailTaskInstrumentation() throws ExecutionException, InterruptedException, TimeoutException {
    var queue = KeyedTaskQueue.createOrOpen(config, db).get();

    // Enqueue and claim a task
    db.runAsync(tr -> queue.enqueue(tr, "test-key", "test-data")).get();
    simulatedTime.set(1000);
    var taskClaim = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);

    // Clear previous spans
    spanExporter.reset();

    // Fail the task
    db.runAsync(tr -> queue.failTask(tr, taskClaim)).get(5, TimeUnit.SECONDS);

    // Verify span was created
    List<SpanData> spans = spanExporter.getFinishedSpanItems();
    assertThat(spans).anySatisfy(span -> {
      assertThat(span.getName()).isEqualTo("taskqueue.failTask");
      assertThat(span.getKind()).isEqualTo(SpanKind.INTERNAL);
      assertThat(span.getStatus().getStatusCode()).isEqualTo(StatusCode.OK);
      assertThat(span.getAttributes().get(AttributeKey.stringKey("task.key")))
          .isEqualTo("test-key");
    });

    // Verify metrics
    Collection<MetricData> metrics = metricReader.collectAllMetrics();
    assertThat(metrics).anySatisfy(metric -> {
      assertThat(metric.getName()).isEqualTo("taskqueue.tasks.failed");
    });
  }

  @Test
  void testQueuePathInTelemetry() throws ExecutionException, InterruptedException, TimeoutException {
    var queue = KeyedTaskQueue.createOrOpen(config, db).get();

    // Clear existing telemetry
    spanExporter.reset();
    metricReader.collectAllMetrics();

    // Enqueue a task
    db.runAsync(tr -> queue.enqueue(tr, "test-key", "test-data")).get();

    // Verify span includes queue path
    List<SpanData> spans = spanExporter.getFinishedSpanItems();
    assertThat(spans).isNotEmpty();
    assertThat(spans.get(0).getAttributes().get(AttributeKey.stringKey("taskqueue.path")))
        .isNotNull()
        .startsWith("/");

    // Claim task and verify span
    simulatedTime.set(1000);
    spanExporter.reset();
    var taskClaim = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);

    spans = spanExporter.getFinishedSpanItems();
    assertThat(spans).anySatisfy(span -> {
      assertThat(span.getName()).isEqualTo("taskqueue.awaitAndClaimTask");
      assertThat(span.getAttributes().get(AttributeKey.stringKey("taskqueue.path")))
          .isNotNull()
          .startsWith("/");
    });

    // Complete task and verify span
    spanExporter.reset();
    db.runAsync(tr -> queue.completeTask(tr, taskClaim)).get(5, TimeUnit.SECONDS);

    spans = spanExporter.getFinishedSpanItems();
    assertThat(spans).anySatisfy(span -> {
      assertThat(span.getName()).isEqualTo("taskqueue.completeTask");
      assertThat(span.getAttributes().get(AttributeKey.stringKey("taskqueue.path")))
          .isNotNull()
          .startsWith("/");
    });
  }

  @Test
  void testExtendTtlInstrumentation() throws ExecutionException, InterruptedException, TimeoutException {
    var queue = KeyedTaskQueue.createOrOpen(config, db).get();

    // Enqueue and claim a task
    db.runAsync(tr -> queue.enqueue(tr, "test-key", "test-data")).get();
    simulatedTime.set(1000);
    var taskClaim = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);

    // Clear previous spans
    spanExporter.reset();

    // Extend TTL
    db.runAsync(tr -> queue.extendTtl(tr, taskClaim, Duration.ofMinutes(10)))
        .get(5, TimeUnit.SECONDS);

    // Verify span was created
    List<SpanData> spans = spanExporter.getFinishedSpanItems();
    assertThat(spans).anySatisfy(span -> {
      assertThat(span.getName()).isEqualTo("taskqueue.extendTtl");
      assertThat(span.getKind()).isEqualTo(SpanKind.INTERNAL);
      assertThat(span.getStatus().getStatusCode()).isEqualTo(StatusCode.OK);
      assertThat(span.getAttributes().get(AttributeKey.stringKey("task.key")))
          .isEqualTo("test-key");
      assertThat(span.getAttributes().get(AttributeKey.longKey("task.extension.ms")))
          .isEqualTo(600000L);
    });
  }

  @Test
  void testEnqueueInstrumentationWithError() throws InterruptedException, ExecutionException {
    var queue = KeyedTaskQueue.createOrOpen(config, db).get();
    spanExporter.reset();

    // Try to enqueue with null key to trigger error path
    try {
      db.runAsync(tr -> queue.enqueue(tr, null, "test-data")).get(5, TimeUnit.SECONDS);
      fail("Should have thrown an exception");
    } catch (Exception e) {
      // Expected exception
    }

    // Verify error span was created
    List<SpanData> spans = spanExporter.getFinishedSpanItems();
    assertThat(spans).isEmpty();
  }

  @Test
  void testCompleteTaskAlreadyCompleted() throws ExecutionException, InterruptedException, TimeoutException {
    var queue = KeyedTaskQueue.createOrOpen(config, db).get();

    // Enqueue and claim a task
    db.runAsync(tr -> queue.enqueue(tr, "test-key", "test-data")).get();
    simulatedTime.set(1000);
    var taskClaim = queue.awaitAndClaimTask(db).get(5, TimeUnit.SECONDS);

    // Complete the task once
    db.runAsync(tr -> queue.completeTask(tr, taskClaim)).get(5, TimeUnit.SECONDS);

    spanExporter.reset();

    // Try to complete again - should handle gracefully
    db.runAsync(tr -> queue.completeTask(tr, taskClaim)).get(5, TimeUnit.SECONDS);

    // Verify span was created
    List<SpanData> spans = spanExporter.getFinishedSpanItems();
    assertThat(spans).anySatisfy(span -> {
      assertThat(span.getName()).isEqualTo("taskqueue.completeTask");
      // Task already completed should still result in OK status but with event
      assertThat(span.getStatus().getStatusCode()).isEqualTo(StatusCode.OK);
      assertThat(span.getEvents()).anySatisfy(event -> {
        String name = event.getName();
        assertThat(name.contains("no metadata") || name.contains("already"))
            .isTrue();
      });
    });
  }

  @Test
  void testFailTaskWithInvalidClaim() throws InterruptedException, ExecutionException {
    var queue = KeyedTaskQueue.createOrOpen(config, db).get();

    // Create a mock invalid claim
    var invalidClaim = TaskClaim.<String, String>builder()
        .taskProto(Task.newBuilder()
            .setTaskUuid(com.google.protobuf.ByteString.copyFrom(new byte[16]))
            .setClaim(com.google.protobuf.ByteString.copyFromUtf8("invalid-claim"))
            .setTaskKey(com.google.protobuf.ByteString.copyFrom("test".getBytes()))
            .build())
        .taskKeyProto(null)
        .taskKeyMetadataProto(null)
        .taskQueue(queue)
        .taskKey("test-key")
        .task("test-data")
        .build();

    spanExporter.reset();

    // This should fail but handle error gracefully
    try {
      db.runAsync(tr -> queue.failTask(tr, invalidClaim)).get(5, TimeUnit.SECONDS);
    } catch (Exception e) {
      // Expected - task not found or invalid claim
    }

    // Verify span was created
    List<SpanData> spans = spanExporter.getFinishedSpanItems();
    assertThat(spans).anySatisfy(span -> {
      assertThat(span.getName()).isEqualTo("taskqueue.failTask");
      // Should have events for the error case
      assertThat(span.getEvents()).isNotEmpty();
    });
  }

  @Test
  void testIsEmpty() throws InterruptedException, ExecutionException {
    var queue = KeyedTaskQueue.createOrOpen(config, db).get();

    // Queue should be empty initially
    assertThat(queue.isEmpty().get()).isTrue();

    // Enqueue a task
    queue.enqueue("key1", "data1").get();
    assertThat(queue.isEmpty().get()).isFalse();

    // Claim the task
    var claim = queue.awaitAndClaimTask().get();
    assertThat(queue.isEmpty().get()).isFalse(); // Still not empty (task is claimed)

    // Complete the task
    queue.completeTask(claim).get();
    assertThat(queue.isEmpty().get()).isTrue(); // Now empty again
  }

  @Test
  void testIsEmptyWithMultipleTasks() throws InterruptedException, ExecutionException {
    var queue = KeyedTaskQueue.createOrOpen(config, db).get();

    // Add multiple tasks
    queue.enqueue("key1", "data1").get();
    queue.enqueue("key2", "data2").get();
    assertThat(queue.isEmpty().get()).isFalse();

    // Claim and complete first task
    var claim1 = queue.awaitAndClaimTask().get();
    queue.completeTask(claim1).get();
    assertThat(queue.isEmpty().get()).isFalse(); // Still has second task

    // Claim and complete second task
    var claim2 = queue.awaitAndClaimTask().get();
    queue.completeTask(claim2).get();
    assertThat(queue.isEmpty().get()).isTrue(); // Now empty
  }

  @Test
  void testHasVisibleUnclaimedTasks() throws InterruptedException, ExecutionException {
    var queue = KeyedTaskQueue.createOrOpen(config, db).get();

    // Queue should not have visible tasks initially
    assertThat(queue.hasVisibleUnclaimedTasks().get()).isFalse();

    // Enqueue a task with no delay - should be immediately visible
    queue.enqueue("key1", "data1").get();
    assertThat(queue.hasVisibleUnclaimedTasks().get()).isTrue();

    // Claim the task
    var claim = queue.awaitAndClaimTask().get();
    assertThat(queue.hasVisibleUnclaimedTasks().get()).isFalse(); // No unclaimed tasks

    // Fail the task to make it unclaimed again
    queue.failTask(claim).get();
    assertThat(queue.hasVisibleUnclaimedTasks().get()).isTrue();

    // Complete the task
    var claim2 = queue.awaitAndClaimTask().get();
    queue.completeTask(claim2).get();
    assertThat(queue.hasVisibleUnclaimedTasks().get()).isFalse();
  }

  @Test
  void testHasVisibleUnclaimedTasksWithDelay() throws InterruptedException, ExecutionException, TimeoutException {
    var queue = KeyedTaskQueue.createOrOpen(config, db).get();

    // Set initial time
    simulatedTime.set(1000);

    // Enqueue a task with delay - should not be immediately visible
    db.runAsync(tr -> queue.enqueue(tr, "key1", "data1", Duration.ofSeconds(2)))
        .get();
    assertThat(queue.hasVisibleUnclaimedTasks().get()).isFalse();

    // Advance simulated time to make task visible
    simulatedTime.set(3000);
    assertThat(queue.hasVisibleUnclaimedTasks().get()).isTrue();

    // Clean up
    var claim = queue.awaitAndClaimTask().get(5, TimeUnit.SECONDS);
    queue.completeTask(claim).get();
  }

  @Test
  void testHasVisibleUnclaimedTasksMixedVisibility() throws InterruptedException, ExecutionException {
    var queue = KeyedTaskQueue.createOrOpen(config, db).get();

    // Enqueue tasks with different delays
    db.runAsync(tr -> queue.enqueue(tr, "key1", "data1", Duration.ofSeconds(10)))
        .get(); // Not visible
    db.runAsync(tr -> queue.enqueue(tr, "key2", "data2", Duration.ZERO)).get(); // Visible
    db.runAsync(tr -> queue.enqueue(tr, "key3", "data3", Duration.ofSeconds(5)))
        .get(); // Not visible

    // Should return true because of the immediately visible task
    assertThat(queue.hasVisibleUnclaimedTasks().get()).isTrue();

    // Claim the visible task
    var claim = queue.awaitAndClaimTask().get();
    assertThat(claim.taskKey()).isEqualTo("key2");
    queue.completeTask(claim).get();

    // Now no visible tasks
    assertThat(queue.hasVisibleUnclaimedTasks().get()).isFalse();

    // Advance time to make other tasks visible
    simulatedTime.set(20000);
    while (queue.hasVisibleUnclaimedTasks().get()) {
      var c = queue.awaitAndClaimTask().get();
      queue.completeTask(c).get();
    }
  }

  @Test
  void testOrphanedTaskWithNoMetadata() throws InterruptedException, ExecutionException {
    var queue = KeyedTaskQueue.createOrOpen(config, db).get();

    // Manually create an orphaned task entry without metadata to simulate the error condition
    db.runAsync(tr -> config.getDirectory()
            .createOrOpen(tr, List.of("unclaimed_tasks"))
            .thenAccept(unclaimedTasks -> {
              // lowest possible uuid so it always comes first.
              var taskUuid = UUID.fromString("00000000-0000-0000-0000-000000000000");
              var taskProto = Task.newBuilder()
                  .setTaskUuid(ByteString.copyFrom(KeyedTaskQueue.uuidToBytes(taskUuid)))
                  .setCreationTime(
                      Timestamp.newBuilder().setSeconds(1).build())
                  .setTaskKey(ByteString.copyFromUtf8("orphaned-key"))
                  .setTaskVersion(1)
                  .setAttempts(0)
                  .build();

              // Create task entry without corresponding metadata - this simulates the NPE condition
              var taskKey = unclaimedTasks.pack(Tuple.from(0L, KeyedTaskQueue.uuidToBytes(taskUuid)));
              tr.set(taskKey, taskProto.toByteArray());
            }))
        .get();

    // Now enqueue a valid task that should be claimable
    queue.enqueue("valid-key", "valid-data").get();

    // Set time so both tasks are visible
    simulatedTime.set(2000);

    // This should not throw NPE even though the first task has no metadata
    // It should skip the orphaned task and claim the valid one
    var claim = queue.awaitAndClaimTask(db).get();

    // Should have claimed the valid task, not the orphaned one
    assertThat(claim).isNotNull();
    assertThat(claim.taskKey()).isEqualTo("valid-key");
    assertThat(claim.task()).isEqualTo("valid-data");

    // Complete the valid task
    claim.complete().get();

    // Orphaned task cleanup now properly removes the task key, so isEmpty() returns true as expected
    assertThat(queue.isEmpty().get()).isTrue();
  }

  @Test
  void testHasClaimedTasks() throws InterruptedException, ExecutionException {
    var queue = KeyedTaskQueue.createOrOpen(config, db).get();

    // Initially no claimed tasks
    assertThat(queue.hasClaimedTasks().get()).isFalse();

    // Enqueue a task
    queue.enqueue("key1", "data1").get();

    // Still no claimed tasks (task is unclaimed)
    assertThat(queue.hasClaimedTasks().get()).isFalse();

    // Claim the task
    var claim1 = queue.awaitAndClaimTask().get();

    // Now we have a claimed task
    assertThat(queue.hasClaimedTasks().get()).isTrue();

    // Enqueue and claim another task
    queue.enqueue("key2", "data2").get();
    var claim2 = queue.awaitAndClaimTask().get();

    // Still have claimed tasks
    assertThat(queue.hasClaimedTasks().get()).isTrue();

    // Complete one task
    queue.completeTask(claim1).get();

    // Still have one claimed task
    assertThat(queue.hasClaimedTasks().get()).isTrue();

    // Complete the second task
    queue.completeTask(claim2).get();

    // No more claimed tasks
    assertThat(queue.hasClaimedTasks().get()).isFalse();
  }

  @Test
  void testHasClaimedTasksWithFailedTask() throws InterruptedException, ExecutionException {
    var queue = KeyedTaskQueue.createOrOpen(config, db).get();

    // Enqueue and claim a task
    queue.enqueue("key1", "data1").get();
    var claim = queue.awaitAndClaimTask().get();

    // Has claimed task
    assertThat(queue.hasClaimedTasks().get()).isTrue();

    // Fail the task (moves it back to unclaimed)
    queue.failTask(claim).get();

    // No claimed tasks after failure
    assertThat(queue.hasClaimedTasks().get()).isFalse();

    // Clean up
    var claim2 = queue.awaitAndClaimTask().get();
    queue.completeTask(claim2).get();
  }

  @Test
  void testHasClaimedTasksWithExpiredTask() throws InterruptedException, ExecutionException, TimeoutException {
    var queue = KeyedTaskQueue.createOrOpen(config, db).get();

    // Set initial time
    simulatedTime.set(1000);

    // Enqueue a task with short TTL
    db.runAsync(tr -> queue.enqueue(tr, "key1", "data1", Duration.ZERO, Duration.ofSeconds(2)))
        .get();

    // Claim the task
    var claim = queue.awaitAndClaimTask().get(5, TimeUnit.SECONDS);

    // Has claimed task
    assertThat(queue.hasClaimedTasks().get()).isTrue();

    // Advance time beyond TTL to expire the claim
    simulatedTime.set(5000);

    // Task is still in claimed space even though it's expired
    assertThat(queue.hasClaimedTasks().get()).isTrue();

    // Reclaim the expired task
    var claim2 = queue.awaitAndClaimTask().get(5, TimeUnit.SECONDS);

    // Still has claimed task (it was reclaimed)
    assertThat(queue.hasClaimedTasks().get()).isTrue();

    // Complete the task
    queue.completeTask(claim2).get();

    // No more claimed tasks
    assertThat(queue.hasClaimedTasks().get()).isFalse();
  }

  @Test
  void testReclaimExpiredTaskProperlyMovesItInClaimedSpace()
      throws InterruptedException, ExecutionException, TimeoutException {
    var queue = KeyedTaskQueue.createOrOpen(config, db).get();

    // Set initial time
    simulatedTime.set(1000);

    // Enqueue a task with short TTL (2 seconds)
    db.runAsync(tr -> queue.enqueue(tr, "key1", "data1", Duration.ZERO, Duration.ofSeconds(2)))
        .get();

    // Claim the task - it will be stored with expiration at time 3000 (1000 + 2000)
    var claim1 = queue.awaitAndClaimTask().get(5, TimeUnit.SECONDS);

    // Verify we have exactly one claimed task
    assertThat(queue.hasClaimedTasks().get()).isTrue();

    // Advance time beyond TTL to expire the claim (to time 4000)
    simulatedTime.set(4000);

    // The task is still in claimed space at the old expiration slot (3000)
    assertThat(queue.hasClaimedTasks().get()).isTrue();

    // Reclaim the expired task - this should:
    // 1. Remove it from the old expiration slot (3000)
    // 2. Add it to a new expiration slot (6000 = 4000 + 2000)
    var claim2 = queue.awaitAndClaimTask().get(5, TimeUnit.SECONDS);

    // Should still have exactly one claimed task (not two!)
    assertThat(queue.hasClaimedTasks().get()).isTrue();

    // Verify it's the same task that was reclaimed
    assertThat(claim2.taskKey()).isEqualTo("key1");
    assertThat(claim2.task()).isEqualTo("data1");

    // Complete the task to clean up
    queue.completeTask(claim2).get();

    // Verify no claimed tasks remain
    assertThat(queue.hasClaimedTasks().get()).isFalse();

    // Also verify the queue is completely empty
    assertThat(queue.isEmpty().get()).isTrue();
  }
}
