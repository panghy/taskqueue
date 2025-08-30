package io.github.panghy.taskqueue;

import static com.apple.foundationdb.tuple.ByteArrayUtil.printable;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.TransactionContext;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import io.github.panghy.taskqueue.proto.Task;
import io.github.panghy.taskqueue.proto.TaskKey;
import io.github.panghy.taskqueue.proto.TaskKeyMetadata;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

/**
 * A distributed task queue library backed by FoundationDB with the ability to deduplicate tasks based on a key.
 *
 * @param <K> the type of task keys
 * @param <T> the type of task data
 */
public class KeyedTaskQueue<K, T> implements TaskQueue<K, T> {

  private static final Logger LOGGER = Logger.getLogger(KeyedTaskQueue.class.getName());
  private static final SecureRandom random = new SecureRandom();

  private static final String METADATA_KEY = "metadata";
  private static final byte[] ONE = new byte[] {0x01};

  // OpenTelemetry instrumentation - instance fields
  private final Tracer tracer;
  private final Meter meter;
  private final LongCounter tasksEnqueued;
  private final LongCounter tasksClaimed;
  private final LongCounter tasksCompleted;
  private final LongCounter tasksFailed;
  private final LongHistogram taskProcessingDuration;
  private final LongHistogram taskWaitTime;

  // Attribute keys for spans and metrics
  private static final AttributeKey<String> TASK_KEY = AttributeKey.stringKey("task.key");
  private static final AttributeKey<String> TASK_UUID = AttributeKey.stringKey("task.uuid");
  private static final AttributeKey<Long> TASK_VERSION = AttributeKey.longKey("task.version");
  private static final AttributeKey<Long> TASK_ATTEMPTS = AttributeKey.longKey("task.attempts");
  private static final AttributeKey<Long> DELAY_MS = AttributeKey.longKey("task.delay.ms");
  private static final AttributeKey<Long> TTL_MS = AttributeKey.longKey("task.ttl.ms");

  private final TaskQueueConfig<K, T> config;
  private final DirectorySubspace unclaimedTasks;
  private final DirectorySubspace claimedTasks;
  private final DirectorySubspace taskKeys;
  private final byte[] watchKey;

  /**
   * Private constructor. Use {@link #createOrOpen(TaskQueueConfig, TransactionContext)} to create an instance.
   */
  private KeyedTaskQueue(
      TaskQueueConfig<K, T> config,
      DirectorySubspace unclaimedTasks,
      DirectorySubspace claimedTasks,
      DirectorySubspace taskKeys,
      byte[] watchKey) {
    this.config = config;
    this.unclaimedTasks = unclaimedTasks;
    this.claimedTasks = claimedTasks;
    this.taskKeys = taskKeys;
    this.watchKey = watchKey;

    // Initialize OpenTelemetry instrumentation
    this.tracer = GlobalOpenTelemetry.getTracer("io.github.panghy.taskqueue", "0.3.0");
    this.meter = GlobalOpenTelemetry.getMeter("io.github.panghy.taskqueue");

    this.tasksEnqueued = meter.counterBuilder("taskqueue.tasks.enqueued")
        .setDescription("Number of tasks enqueued")
        .setUnit("tasks")
        .build();

    this.tasksClaimed = meter.counterBuilder("taskqueue.tasks.claimed")
        .setDescription("Number of tasks claimed by workers")
        .setUnit("tasks")
        .build();

    this.tasksCompleted = meter.counterBuilder("taskqueue.tasks.completed")
        .setDescription("Number of tasks completed successfully")
        .setUnit("tasks")
        .build();

    this.tasksFailed = meter.counterBuilder("taskqueue.tasks.failed")
        .setDescription("Number of tasks that failed")
        .setUnit("tasks")
        .build();

    this.taskProcessingDuration = meter.histogramBuilder("taskqueue.task.processing.duration")
        .setDescription("Duration of task processing")
        .setUnit("ms")
        .ofLongs()
        .build();

    this.taskWaitTime = meter.histogramBuilder("taskqueue.task.wait.time")
        .setDescription("Time tasks wait before being claimed")
        .setUnit("ms")
        .ofLongs()
        .build();
  }

  /**
   * Opens a task queue.
   *
   * @param config  the configuration for the task queue
   * @param context the transaction context
   * @param <K>     the type of the task key
   * @param <T>     the type of the task data
   * @return a future that completes with the task queue
   */
  static <K, T> CompletableFuture<TaskQueue<K, T>> createOrOpen(
      TaskQueueConfig<K, T> config, TransactionContext context) {
    var unclaimedTaskF = config.getDirectory().createOrOpen(context, List.of("unclaimed_tasks"));
    var claimedTaskF = config.getDirectory().createOrOpen(context, List.of("claimed_tasks"));
    var taskKeyF = config.getDirectory().createOrOpen(context, List.of("task_keys"));
    var watchKeyF =
        config.getDirectory().createOrOpen(context, List.of("watch")).thenApply(Subspace::getKey);
    var watchKeyWriteF = watchKeyF.thenAccept(watchKey -> context.run(tr -> {
      tr.set(watchKey, ONE);
      return null;
    }));
    return allOf(unclaimedTaskF, claimedTaskF, taskKeyF, watchKeyF, watchKeyWriteF)
        .thenApply(v -> new KeyedTaskQueue<>(
            config, unclaimedTaskF.join(), claimedTaskF.join(), taskKeyF.join(), watchKeyF.join()));
  }

  @Override
  public TaskQueueConfig<K, T> getConfig() {
    return config;
  }

  @Override
  public CompletableFuture<TaskKeyMetadata> enqueueIfNotExists(
      Transaction tr, K taskKey, T task, Duration delay, Duration ttl, boolean enqueueIfAlreadyRunning) {
    ByteString taskKeyBytes = config.getKeySerializer().serialize(taskKey);
    var metadataDataF = getTaskMetadataAsync(tr, taskKeyBytes);
    return metadataDataF.thenCompose(metadataProto -> {
      if (metadataProto == null) {
        // this is the first time we have seen this task key.
        return enqueue(tr, taskKey, task, delay, ttl);
      } else if (enqueueIfAlreadyRunning
          && metadataProto.hasCurrentClaim()
          && metadataProto.getCurrentClaim().getVersion() == metadataProto.getHighestVersionSeen()) {
        // the task is already running, but we want to enqueue a new task.
        return enqueue(tr, taskKey, task, delay, ttl);
      }
      return completedFuture(metadataProto);
    });
  }

  @Override
  public CompletableFuture<TaskKeyMetadata> enqueue(Transaction tr, K taskKey, T task, Duration delay, Duration ttl) {
    // Check for nulls.
    if (taskKey == null || task == null) {
      throw new IllegalArgumentException("taskKey and task cannot be null");
    }
    if (delay == null || ttl == null) {
      throw new IllegalArgumentException("delay and ttl cannot be null");
    }
    if (delay.isNegative() || ttl.isNegative()) {
      throw new IllegalArgumentException("delay and ttl cannot be negative");
    }
    // Start a span for enqueue operation
    Span span = tracer.spanBuilder("taskqueue.enqueue")
        .setSpanKind(SpanKind.PRODUCER)
        .setAttribute(TASK_KEY, String.valueOf(taskKey))
        .setAttribute(DELAY_MS, delay.toMillis())
        .setAttribute(TTL_MS, ttl.toMillis())
        .startSpan();

    // unique task uuid.
    UUID taskUuid = UUID.randomUUID();
    ByteString taskUuidBS = ByteString.copyFrom(uuidToBytes(taskUuid));
    span.setAttribute(TASK_UUID, taskUuid.toString());

    // when should the task be made visible to workers.
    Instant visibleTime = config.getInstantSource().instant().plus(delay);
    // task keys allow for at-most-one task per taskKey semantics.
    ByteString taskKeyBytes = config.getKeySerializer().serialize(taskKey);
    byte[] taskKeyB = taskKeyBytes.toByteArray();
    byte[] metadataKeyB = taskKeys.pack(Tuple.from(taskKeyB, METADATA_KEY));
    // probe metadata to see if we have seen this task key before.
    var metadataDataF = getTaskMetadataAsync(tr, taskKeyBytes);
    // update metadata.
    var setMetadataF = metadataDataF.thenApply(metadataProto -> {
      if (metadataProto == null) {
        // this is the first time we have seen this task key.
        metadataProto =
            TaskKeyMetadata.newBuilder().setHighestVersionSeen(1).build();
      } else {
        // task with the key might already be claimed, or it's still waiting for execution.
        // the later case is possible when a new version of a task was submitted while an older version was
        // running.
        metadataProto = metadataProto.toBuilder()
            .setHighestVersionSeen(metadataProto.getHighestVersionSeen() + 1)
            .build();
      }
      tr.set(metadataKeyB, metadataProto.toByteArray());
      return metadataProto;
    });
    // Store the task in the unclaimed space as well as the task key.
    var taskF = setMetadataF.thenApply(taskMetadata -> {
      span.setAttribute(TASK_VERSION, taskMetadata.getHighestVersionSeen());
      TaskKey taskKeyProto = createNewTaskKeyProto(taskUuidBS, task, ttl, visibleTime);
      storeTaskKey(tr, taskKeyB, taskMetadata.getHighestVersionSeen(), taskKeyProto);
      if (!taskMetadata.hasCurrentClaim()) {
        Task taskProto = createNewTaskProto(
            taskUuidBS, taskKeyBytes, setMetadataF.join().getHighestVersionSeen());
        storeUnclaimedTask(tr, visibleTime, taskUuid, taskProto);
        span.addEvent("Task stored in unclaimed queue");
      } else {
        span.addEvent("Task has current claim, stored as new version");
      }
      return null;
    });
    return taskF.thenApply(v -> {
          LOGGER.info("Enqueued task: " + describeTask(taskUuid, task));
          TaskKeyMetadata result = setMetadataF.join();

          // Record metrics
          tasksEnqueued.add(1);

          span.setStatus(StatusCode.OK).end();
          return result;
        })
        .exceptionally(error -> {
          span.recordException(error)
              .setStatus(StatusCode.ERROR, error.getMessage())
              .end();
          throw new RuntimeException(error);
        });
  }

  @Override
  public CompletableFuture<TaskClaim<K, T>> awaitAndClaimTask(Database db) {
    Span span = tracer.spanBuilder("taskqueue.awaitAndClaimTask")
        .setSpanKind(SpanKind.CONSUMER)
        .startSpan();

    AtomicReference<TaskClaim<K, T>> ref = new AtomicReference<>();
    return AsyncUtil.whileTrue(
            () -> {
              var watchFF = db.runAsync(tr -> {
                ref.set(null);
                var unclaimedTaskOptF = findUnclaimedTask(tr);
                return unclaimedTaskOptF.thenCompose(claimedTaskO -> {
                  if (claimedTaskO.isPresent()) {
                    ref.set(claimedTaskO.get());
                    return completedFuture(null);
                  }
                  return findAndReclaimExpiredTask(tr).thenApply(taskO -> {
                    if (taskO.isPresent()) {
                      ref.set(taskO.get());
                      return null;
                    }
                    return tr.watch(watchKey);
                  });
                });
              });
              // find the next expiration time.
              var nextExpirationOF = db.runAsync(tr -> {
                // either unclaimed becomes visible or claimed expires.
                var unclaimedF = findNextExpiration(tr, unclaimedTasks);
                var claimedF = findNextExpiration(tr, claimedTasks);
                return allOf(unclaimedF, claimedF).thenApply(v -> {
                  // take the earlier one if available.
                  Optional<Instant> earliestUnclaimedO = unclaimedF.join();
                  Optional<Instant> earliestClaimedO = claimedF.join();
                  if (earliestUnclaimedO.isPresent() && earliestClaimedO.isPresent()) {
                    return earliestUnclaimedO.get().compareTo(earliestClaimedO.get()) < 0
                        ? earliestUnclaimedO
                        : earliestClaimedO;
                  } else if (earliestUnclaimedO.isPresent()) {
                    return earliestUnclaimedO;
                  } else return earliestClaimedO;
                });
              });
              // txn has committed, watch is now active.
              return watchFF.thenCompose(watchF -> {
                // watchF is null if a task was found.
                if (watchF != null) {
                  return nextExpirationOF.thenCompose(nextExpirationO -> {
                    if (nextExpirationO.isPresent()) {
                      long sleepTime =
                          nextExpirationO.get().toEpochMilli()
                              - config.getInstantSource()
                                  .instant()
                                  .toEpochMilli();
                      if (sleepTime > 0) {
                        return watchF.orTimeout(sleepTime, TimeUnit.MILLISECONDS)
                            .exceptionally($ -> null)
                            .thenApply($ -> true);
                      }
                      return completedFuture(true);
                    }
                    return watchF.thenApply($ -> true);
                  });
                }
                return completedFuture(false);
              });
            },
            db.getExecutor())
        .thenApply(v -> {
          TaskClaim<K, T> claim = ref.get();
          if (claim != null) {
            span.setAttribute(TASK_UUID, claim.getTaskUuid().toString())
                .setAttribute(TASK_KEY, String.valueOf(claim.taskKey()))
                .setAttribute(TASK_VERSION, claim.getTaskVersion())
                .setAttribute(TASK_ATTEMPTS, claim.getAttempts());

            // Record wait time if we have creation time
            if (claim.taskProto().hasCreationTime()) {
              long waitTime = config.getInstantSource().instant().toEpochMilli()
                  - toJavaTimestamp(claim.taskProto().getCreationTime())
                      .toEpochMilli();
              taskWaitTime.record(waitTime);
              span.setAttribute("task.wait.time.ms", waitTime);
            }

            tasksClaimed.add(1);
            span.addEvent("Task claimed successfully")
                .setStatus(StatusCode.OK)
                .end();
          } else {
            span.setStatus(StatusCode.ERROR, "Failed to claim task").end();
          }
          return claim;
        })
        .exceptionally(error -> {
          span.recordException(error)
              .setStatus(StatusCode.ERROR, error.getMessage())
              .end();
          throw new CompletionException(error);
        });
  }

  @Override
  public CompletableFuture<Void> completeTask(Transaction tr, TaskClaim<K, T> taskClaim) {
    Span span = tracer.spanBuilder("taskqueue.completeTask")
        .setSpanKind(SpanKind.INTERNAL)
        .setAttribute(TASK_UUID, taskClaim.getTaskUuid().toString())
        .setAttribute(TASK_KEY, String.valueOf(taskClaim.taskKey()))
        .setAttribute(TASK_VERSION, taskClaim.getTaskVersion())
        .setAttribute(TASK_ATTEMPTS, taskClaim.getAttempts())
        .startSpan();

    ByteString taskKeyBytes = taskClaim.taskProto().getTaskKey();
    var taskMetadataF = getTaskMetadataAsync(tr, taskKeyBytes);
    UUID taskUuid = bytesToUuid(taskClaim.taskProto().getTaskUuid().toByteArray());
    return taskMetadataF
        .thenCompose(taskMetadataProto -> {
          if (taskMetadataProto == null) {
            // likely a retry of a task that has already been completed.
            LOGGER.warning(
                "Task " + describeTask(taskUuid, taskClaim.task()) + " has no metadata. Skipping.");
            span.addEvent("Task has no metadata. Skipping.")
                .setStatus(StatusCode.OK)
                .end();
            return completedFuture(null);
          }
          if (taskMetadataProto.hasCurrentClaim()
              && !taskMetadataProto
                  .getCurrentClaim()
                  .getClaim()
                  .equals(taskClaim.taskProto().getClaim())) {
            // task likely took too long to complete and another worker already picked it up.
            LOGGER.warning("Task " + describeTask(taskUuid, taskClaim.task())
                + " is not the current claim. Skipping.");
            span.addEvent("Task is not the current claim. Skipping.")
                .setStatus(StatusCode.OK)
                .end();
            return completedFuture(null);
          }
          // remove the task from claimed space.
          Instant currentExpiration =
              toJavaTimestamp(taskMetadataProto.getCurrentClaim().getExpirationTime());
          tr.clear(claimedTasks.pack(Tuple.from(currentExpiration.toEpochMilli(), taskUuid)));
          byte[] taskKeyB = taskKeyBytes.toByteArray();
          if (taskClaim.taskProto().getTaskVersion() == taskMetadataProto.getHighestVersionSeen()) {
            LOGGER.info("Completing task: " + describeTask(taskUuid, taskClaim.task()));
            // remove all versions of the task (+ metadata).
            tr.clear(Range.startsWith(taskKeys.pack(taskKeyB)));
            return completedFuture(null);
          } else {
            // another version of the task needs to be scheduled.
            var taskKeyF = getTaskKeyAsync(tr, taskKeyBytes, taskMetadataProto.getHighestVersionSeen());
            return taskKeyF.thenApply(taskKeyProto -> {
              LOGGER.info("Scheduling next version of task: " + describeTask(taskUuid, taskClaim.task())
                  + ": "
                  + taskClaim.taskProto().getTaskVersion() + " -> "
                  + taskMetadataProto.getHighestVersionSeen());
              Instant expectedExecutionTime = toJavaTimestamp(taskKeyProto.getExpectedExecutionTime());
              if (expectedExecutionTime.isBefore(
                  config.getInstantSource().instant())) {
                // the task is already due, we'll apply the throttle if there is one.
                expectedExecutionTime = config.getInstantSource()
                    .instant()
                    .plus(toJavaDuration(taskKeyProto.getThrottle()));
              }
              Task newTaskProto = createNewTaskProto(
                  taskKeyProto.getTaskUuid(),
                  taskKeyBytes,
                  taskMetadataProto.getHighestVersionSeen());
              storeUnclaimedTask(
                  tr,
                  expectedExecutionTime,
                  bytesToUuid(taskKeyProto.getTaskUuid().toByteArray()),
                  newTaskProto);
              TaskKeyMetadata updatedTaskKeyMetadataProto = taskMetadataProto.toBuilder()
                  .clearCurrentClaim()
                  .build();
              storeTaskMetadata(tr, taskKeyB, updatedTaskKeyMetadataProto);
              return null;
            });
          }
        })
        .handle((result, error) -> {
          if (error != null) {
            span.recordException(error)
                .setStatus(StatusCode.ERROR, error.getMessage())
                .end();
            throw new CompletionException(error);
          } else {
            // Record processing duration if we have creation time
            if (taskClaim.taskProto().hasCreationTime()) {
              long duration = config.getInstantSource().instant().toEpochMilli()
                  - toJavaTimestamp(taskClaim.taskProto().getCreationTime())
                      .toEpochMilli();
              taskProcessingDuration.record(duration);
              span.setAttribute("task.processing.duration.ms", duration);
            }

            tasksCompleted.add(1);
            span.addEvent("Task completed successfully")
                .setStatus(StatusCode.OK)
                .end();
            return null;
          }
        });
  }

  @Override
  public CompletableFuture<Void> extendTtl(Transaction tr, TaskClaim<K, T> taskClaim, Duration extension) {
    if (extension.isNegative() || extension.isZero()) {
      throw new IllegalArgumentException("Extension duration must be positive");
    }

    Span span = tracer.spanBuilder("taskqueue.extendTtl")
        .setSpanKind(SpanKind.INTERNAL)
        .setAttribute(TASK_UUID, taskClaim.getTaskUuid().toString())
        .setAttribute(TASK_KEY, String.valueOf(taskClaim.taskKey()))
        .setAttribute("task.extension.ms", extension.toMillis())
        .startSpan();

    ByteString taskKeyBytes = taskClaim.taskProto().getTaskKey();
    var taskMetadataF = getTaskMetadataAsync(tr, taskKeyBytes);
    UUID taskUuid = bytesToUuid(taskClaim.taskProto().getTaskUuid().toByteArray());
    return taskMetadataF
        .thenCompose(taskMetadataProto -> {
          if (taskMetadataProto == null) {
            // likely a retry of a task that has already been completed.
            LOGGER.warning(
                "Task " + describeTask(taskUuid, taskClaim.task()) + " has no metadata. Skipping.");
            return completedFuture(null);
          }
          // Check if the task has been claimed by another worker
          if (taskMetadataProto.hasCurrentClaim()
              && !taskMetadataProto
                  .getCurrentClaim()
                  .getClaim()
                  .equals(taskClaim.taskProto().getClaim())) {
            // task has been claimed by another worker - throw exception
            throw new TaskQueueException("Task " + describeTask(taskUuid, taskClaim.task())
                + " has been claimed by another worker. Cannot extend TTL.");
          }

          // Calculate new expiration time
          Instant newExpiration = config.getInstantSource().instant().plus(extension);

          // If task is in claimed space, update its expiration
          if (taskMetadataProto.hasCurrentClaim()) {
            // Remove from old expiration slot
            Instant currentExpiration = toJavaTimestamp(
                taskMetadataProto.getCurrentClaim().getExpirationTime());
            tr.clear(claimedTasks.pack(Tuple.from(currentExpiration.toEpochMilli(), taskUuid)));

            // Add to new expiration slot
            storeClaimedTask(tr, newExpiration, taskUuid, taskClaim.taskProto());

            // Update metadata with new expiration
            var updatedMetadata = taskMetadataProto.toBuilder()
                .setCurrentClaim(taskMetadataProto.getCurrentClaim().toBuilder()
                    .setExpirationTime(fromJavaTimestamp(newExpiration))
                    .build())
                .build();
            byte[] taskKeyB = taskKeyBytes.toByteArray();
            storeTaskMetadata(tr, taskKeyB, updatedMetadata);

            LOGGER.info("Extended TTL for task: " + describeTask(taskUuid, taskClaim.task()) + " to "
                + newExpiration);
          } else {
            // This should never happen - a worker with an active claim found metadata with no current claim
            throw new TaskQueueException("Task " + describeTask(taskUuid, taskClaim.task())
                + " has inconsistent state: worker has claim but metadata shows no current claim");
          }

          return completedFuture(null);
        })
        .handle((result, error) -> {
          if (error != null) {
            span.recordException(error)
                .setStatus(StatusCode.ERROR, error.getMessage())
                .end();
            throw new CompletionException(error);
          } else {
            span.addEvent("TTL extended successfully")
                .setStatus(StatusCode.OK)
                .end();
            return null;
          }
        });
  }

  @Override
  public CompletableFuture<Void> failTask(Transaction tr, TaskClaim<K, T> taskClaim) {
    Span span = tracer.spanBuilder("taskqueue.failTask")
        .setSpanKind(SpanKind.INTERNAL)
        .setAttribute(TASK_UUID, taskClaim.getTaskUuid().toString())
        .setAttribute(TASK_KEY, String.valueOf(taskClaim.taskKey()))
        .setAttribute(TASK_VERSION, taskClaim.getTaskVersion())
        .setAttribute(TASK_ATTEMPTS, taskClaim.getAttempts())
        .startSpan();

    ByteString taskKeyBytes = taskClaim.taskProto().getTaskKey();
    var taskMetadataF = getTaskMetadataAsync(tr, taskKeyBytes);
    UUID taskUuid = bytesToUuid(taskClaim.taskProto().getTaskUuid().toByteArray());
    LOGGER.info("Failing task: " + describeTask(taskUuid, taskClaim.task()));
    return taskMetadataF
        .thenCompose(taskMetadataProto -> {
          if (taskMetadataProto == null) {
            // likely a retry of a task that has already been completed.
            LOGGER.warning(
                "Task " + describeTask(taskUuid, taskClaim.task()) + " has no metadata. Skipping.");
            return completedFuture(null);
          }
          if (taskMetadataProto.hasCurrentClaim()
              && !taskMetadataProto
                  .getCurrentClaim()
                  .getClaim()
                  .equals(taskClaim.taskProto().getClaim())) {
            // task likely took too long to complete and another worker already picked it up.
            LOGGER.warning("Task " + describeTask(taskUuid, taskClaim.task())
                + " is not the current claim. Skipping.");
            return completedFuture(null);
          }
          // remove the task from claimed space.
          Instant currentExpiration =
              toJavaTimestamp(taskMetadataProto.getCurrentClaim().getExpirationTime());
          tr.clear(claimedTasks.pack(Tuple.from(currentExpiration.toEpochMilli(), taskUuid)));
          // see if we are still the latest version.
          byte[] taskKeyB = taskKeyBytes.toByteArray();
          if (taskClaim.taskProto().getTaskVersion() == taskMetadataProto.getHighestVersionSeen()) {
            // we are the latest version, fail the task.
            if (taskClaim.taskProto().getAttempts() >= config.getMaxAttempts()) {
              // we have reached the max attempts, clear the task.
              LOGGER.info(
                  "Task " + describeTask(taskUuid, taskClaim.task()) + " has reached max attempts: "
                      + taskClaim.taskProto().getAttempts() + ". Skipping.");
              // clear all versions of the task (+ metadata).
              tr.clear(Range.startsWith(taskKeys.pack(taskKeyB)));
            } else {
              LOGGER.info("Failing task: " + describeTask(taskUuid, taskClaim.task()) + " with "
                  + taskClaim.taskProto().getAttempts()
                  + " attempts. Rescheduling for future execution.");
              var updatedTaskProto = taskClaim.taskProto().toBuilder()
                  .clearClaim()
                  .build();
              Instant visibleTime =
                  toJavaTimestamp(taskClaim.taskKeyProto().getExpectedExecutionTime());
              storeUnclaimedTask(tr, visibleTime, taskUuid, updatedTaskProto);
              var updatedTaskMetadataProto = taskMetadataProto.toBuilder()
                  .clearCurrentClaim()
                  .build();
              storeTaskMetadata(tr, taskKeyB, updatedTaskMetadataProto);
            }
            return completedFuture(null);
          } else {
            // we are not the latest version, fail the task and schedule the latest version.
            LOGGER.info("Task " + describeTask(taskUuid, taskClaim.task()) + " is not the latest version: "
                + taskClaim.taskProto().getTaskVersion()
                + " != " + taskMetadataProto.getHighestVersionSeen() + ". Skipping to latest version.");
            var latestTaskKeyF =
                getTaskKeyAsync(tr, taskKeyBytes, taskMetadataProto.getHighestVersionSeen());
            return latestTaskKeyF.thenApply(latestTaskKey -> {
              Instant visibleTime = toJavaTimestamp(latestTaskKey.getExpectedExecutionTime());
              var updatedTaskProto = createNewTaskProto(
                  latestTaskKey.getTaskUuid(),
                  taskKeyBytes,
                  taskMetadataProto.getHighestVersionSeen());
              storeUnclaimedTask(
                  tr,
                  visibleTime,
                  bytesToUuid(latestTaskKey.getTaskUuid().toByteArray()),
                  updatedTaskProto);
              var updatedTaskMetadataProto = taskMetadataProto.toBuilder()
                  .clearCurrentClaim()
                  .build();
              storeTaskMetadata(tr, taskKeyB, updatedTaskMetadataProto);
              return null;
            });
          }
        })
        .handle((result, error) -> {
          if (error != null) {
            span.recordException(error)
                .setStatus(StatusCode.ERROR, error.getMessage())
                .end();
            throw new CompletionException(error);
          } else {
            tasksFailed.add(1);

            span.addEvent("Task marked as failed", Attributes.of(TASK_ATTEMPTS, taskClaim.getAttempts()))
                .setStatus(StatusCode.OK)
                .end();
            return null;
          }
        });
  }

  private CompletableFuture<Optional<TaskClaim<K, T>>> findUnclaimedTask(Transaction tr) {
    // snapshot read to pick a task. task must be visible to us.
    var taskKVF = snapshotPickTaskFromRange(tr, unclaimedTasks);
    return taskKVF.thenCompose(taskKV -> {
      if (taskKV == null) {
        return completedFuture(Optional.empty());
      }
      // lock the task by adding a read conflict key.
      tr.addReadConflictKey(taskKV.getKey());
      Task taskProto;
      try {
        taskProto = Task.parseFrom(taskKV.getValue());
      } catch (InvalidProtocolBufferException e) {
        throw new TaskQueueException("parse task proto failed for: " + printable(taskKV.getKey()), e);
      }
      if (taskProto.hasClaim()) {
        throw new TaskQueueException(
            "task already claimed (but found in unclaimed space): " + printable(taskKV.getKey()));
      }
      ByteString taskKey = taskProto.getTaskKey();
      var taskKeyMetadataF = getTaskMetadataAsync(tr, taskKey);
      var taskKeyF = taskKeyMetadataF.thenCompose(
          metadata -> getTaskKeyAsync(tr, taskKey, metadata.getHighestVersionSeen()));
      return allOf(taskKeyF, taskKeyMetadataF).thenApply(v -> {
        TaskKey taskKeyProto = taskKeyF.join();
        TaskKeyMetadata taskKeyMetadataProto = taskKeyMetadataF.join();
        if (taskKeyMetadataProto.hasCurrentClaim()) {
          throw new TaskQueueException(
              "task already claimed (but found in unclaimed space): " + printable(taskKV.getKey()));
        }
        T taskObj = config.getTaskSerializer().deserialize(taskKeyProto.getTask());
        UUID taskUuid = bytesToUuid(taskProto.getTaskUuid().toByteArray());
        LOGGER.info("Found unclaimed task: " + describeTask(taskUuid, taskObj));
        UUID claimUuid = UUID.randomUUID();
        ByteString claimUuidBytes = ByteString.copyFrom(uuidToBytes(claimUuid));
        long attempts = taskProto.getAttempts();
        if (taskProto.getAttempts() >= config.getMaxAttempts()) {
          LOGGER.info("Task " + describeTask(taskUuid, taskObj) + " has reached max attempts: " + attempts);
          tr.clear(taskKV.getKey());
          // this technically does not mean that there are no unclaimed tasks but we need to go through
          // another round to find one. we should ideally not re-insert tasks into the queue if they have
          // reached max
          // attempts.
          return Optional.empty();
        } else if (taskProto.getTaskVersion() != taskKeyMetadataProto.getHighestVersionSeen()) {
          LOGGER.info("Task " + describeTask(taskUuid, taskObj) + " is not the latest version: "
              + taskProto.getTaskVersion()
              + " != " + taskKeyMetadataProto.getHighestVersionSeen() + ". Skipping to latest version.");
          attempts = 0;
        }
        attempts++;
        var updatedTaskProto = taskProto.toBuilder()
            .setClaim(claimUuidBytes)
            .setTaskVersion(taskKeyMetadataProto.getHighestVersionSeen())
            .setAttempts(attempts)
            .build();
        // ttlDeadlineMs is the deadline for the task to be completed.
        Instant deadline = config.getInstantSource().instant().plus(toJavaDuration(taskKeyProto.getTtl()));
        var updatedTaskKeyMetadataProto = taskKeyMetadataProto.toBuilder()
            .setCurrentClaim(TaskKeyMetadata.CurrentClaim.newBuilder()
                .setClaim(claimUuidBytes)
                .setVersion(taskKeyMetadataProto.getHighestVersionSeen())
                .setExpirationTime(fromJavaTimestamp(deadline))
                .build())
            .build();
        // move task to claimed space.
        tr.clear(taskKV.getKey());
        storeClaimedTask(tr, deadline, taskUuid, updatedTaskProto);
        byte[] taskKeyB = taskKey.toByteArray();
        storeTaskMetadata(tr, taskKeyB, updatedTaskKeyMetadataProto);
        LOGGER.info("Claiming task: " + describeTask(taskUuid, taskObj) + " with claim: " + claimUuid);
        return Optional.of(TaskClaim.<K, T>builder()
            .taskProto(updatedTaskProto)
            .taskKeyProto(taskKeyProto)
            .taskKeyMetadataProto(updatedTaskKeyMetadataProto)
            .taskQueue(this)
            .taskKey(config.getKeySerializer().deserialize(taskKey))
            .task(config.getTaskSerializer().deserialize(taskKeyProto.getTask()))
            .build());
      });
    });
  }

  private CompletableFuture<KeyValue> snapshotPickTaskFromRange(Transaction tr, DirectorySubspace subspace) {
    return tr.snapshot()
        .getRange(
            subspace.pack(0),
            subspace.pack(config.getInstantSource().instant().toEpochMilli() + 1),
            config.getEstimatedWorkerCount())
        .asList()
        .thenApply(v -> {
          if (v == null || v.isEmpty()) {
            return null;
          }
          // randomly choose one.
          return v.get(random.nextInt(v.size()));
        });
  }

  private CompletableFuture<Optional<Instant>> findNextExpiration(Transaction tr, DirectorySubspace subspace) {
    return tr.snapshot().getRange(subspace.range(), 1).asList().thenApply(v -> {
      if (v == null || v.isEmpty()) {
        return Optional.empty();
      }
      return Optional.of(
          Instant.ofEpochMilli(Tuple.fromBytes(v.get(0).getKey()).getLong(0)));
    });
  }

  private CompletableFuture<Optional<TaskClaim<K, T>>> findAndReclaimExpiredTask(Transaction tr) {
    // snapshot read to pick a task. task must be visible to us.
    var taskKVF = snapshotPickTaskFromRange(tr, claimedTasks);
    return taskKVF.thenCompose(taskKV -> {
      if (taskKV == null) {
        return completedFuture(Optional.empty());
      }
      // lock the task by adding a read conflict key.
      tr.addReadConflictKey(taskKV.getKey());
      Task taskProto;
      try {
        taskProto = Task.parseFrom(taskKV.getValue());
      } catch (InvalidProtocolBufferException e) {
        throw new TaskQueueException("parse task proto failed for: " + printable(taskKV.getKey()), e);
      }
      if (!taskProto.hasClaim()) {
        throw new TaskQueueException(
            "task not claimed (but found in claimed space): " + printable(taskKV.getKey()));
      }
      ByteString taskKeyBytes = taskProto.getTaskKey();
      var taskKeyMetadataF = getTaskMetadataAsync(tr, taskKeyBytes);
      var highestTaskKeyF = taskKeyMetadataF.thenCompose(taskKeyMetadataProto ->
          getTaskKeyAsync(tr, taskKeyBytes, taskKeyMetadataProto.getHighestVersionSeen()));
      return allOf(highestTaskKeyF, taskKeyMetadataF).thenApply(v -> {
        TaskKey latestTaskKey = highestTaskKeyF.join();
        TaskKeyMetadata taskKeyMetadataProto = taskKeyMetadataF.join();
        if (taskKeyMetadataProto.hasCurrentClaim()) {
          if (!taskKeyMetadataProto.getCurrentClaim().getClaim().equals(taskProto.getClaim())) {
            throw new TaskQueueException(
                "task claim mismatch (in claimed space): " + printable(taskKV.getKey()));
          } else if (taskKeyMetadataProto.getCurrentClaim().getVersion() != taskProto.getTaskVersion()) {
            throw new TaskQueueException(
                "task version mismatch (in claimed space): " + printable(taskKV.getKey()));
          }
        } else {
          LOGGER.warning("Task " + printable(taskKV.getKey())
              + " has no current claim but is in claimed space. " + "Reclaiming.");
        }
        T taskObj = config.getTaskSerializer().deserialize(latestTaskKey.getTask());
        UUID taskUuid = bytesToUuid(taskProto.getTaskUuid().toByteArray());
        LOGGER.info("Found expired claimed task: " + describeTask(taskUuid, taskObj));
        // reclaim the task using a new claim UUID.
        UUID claimUuid = UUID.randomUUID();
        ByteString claimUuidBytes = ByteString.copyFrom(uuidToBytes(claimUuid));
        long attempts = taskProto.getAttempts();
        if (taskProto.getAttempts() >= config.getMaxAttempts()) {
          LOGGER.info("Task " + describeTask(taskUuid, taskObj) + " has reached max attempts: " + attempts);
          tr.clear(taskKV.getKey());
          return Optional.empty();
        } else if (taskProto.getTaskVersion() != taskKeyMetadataProto.getHighestVersionSeen()) {
          LOGGER.info("Task " + describeTask(taskUuid, taskObj) + " is not the latest version: "
              + taskProto.getTaskVersion()
              + " != " + taskKeyMetadataProto.getHighestVersionSeen() + ". Skipping to latest version.");
          attempts = 0;
        }
        attempts++;
        var updatedTaskProto = taskProto.toBuilder()
            .setClaim(claimUuidBytes)
            .setTaskVersion(taskKeyMetadataProto.getHighestVersionSeen())
            .setAttempts(attempts)
            .build();
        var deadline = config.getInstantSource().instant().plus(toJavaDuration(latestTaskKey.getTtl()));
        var updatedTaskKeyMetadataProto = taskKeyMetadataProto.toBuilder()
            .setCurrentClaim(TaskKeyMetadata.CurrentClaim.newBuilder()
                .setClaim(claimUuidBytes)
                .setVersion(taskKeyMetadataProto.getHighestVersionSeen())
                .setExpirationTime(fromJavaTimestamp(deadline)))
            .build();
        byte[] taskKeyB = taskKeyBytes.toByteArray();
        storeTaskMetadata(tr, taskKeyB, updatedTaskKeyMetadataProto);
        tr.set(taskKV.getKey(), updatedTaskProto.toByteArray());
        LOGGER.info("Reclaiming task: " + describeTask(taskUuid, taskObj) + " with claim: " + claimUuid);
        return Optional.of(TaskClaim.<K, T>builder()
            .taskProto(updatedTaskProto)
            .taskKeyProto(latestTaskKey)
            .taskKeyMetadataProto(taskKeyMetadataProto)
            .taskQueue(this)
            .taskKey(config.getKeySerializer().deserialize(taskKeyBytes))
            .task(taskObj)
            .build());
      });
    });
  }

  private CompletableFuture<TaskKey> getTaskKeyAsync(Transaction tr, ByteString taskKey, long version) {
    byte[] taskKeyByteArray = taskKey.toByteArray();
    return tr.get(taskKeys.pack(Tuple.from(taskKeyByteArray, version))).thenApply(v -> {
      if (v == null) {
        throw new TaskQueueException(
            "TaskKey not found for key: " + printable(taskKeyByteArray) + " version: " + version);
      }
      try {
        return TaskKey.parseFrom(v);
      } catch (InvalidProtocolBufferException e) {
        throw new TaskQueueException("Failed to parse TaskKey at: " + printable(taskKeyByteArray), e);
      }
    });
  }

  private void incrementWatchKey(Transaction tr) {
    tr.mutate(MutationType.ADD, watchKey, ONE);
  }

  private CompletableFuture<TaskKeyMetadata> getTaskMetadataAsync(Transaction tr, ByteString taskKey) {
    byte[] metadataKeyB = taskKeys.pack(Tuple.from(taskKey.toByteArray(), METADATA_KEY));
    return tr.get(metadataKeyB).thenApply(v -> {
      try {
        return v == null ? null : TaskKeyMetadata.parseFrom(v);
      } catch (InvalidProtocolBufferException e) {
        throw new TaskQueueException("Failed to parse TaskKeyMetadata at: " + printable(metadataKeyB), e);
      }
    });
  }

  private Task createNewTaskProto(ByteString taskUuidBytes, ByteString taskKeyBytes, long version) {
    return Task.newBuilder()
        .setTaskUuid(taskUuidBytes)
        .setCreationTime(fromJavaTimestamp(config.getInstantSource().instant()))
        .setTaskKey(taskKeyBytes)
        .setTaskVersion(version)
        .build();
  }

  /**
   * Stores task metadata in the database.
   *
   * @param tr       the transaction to use
   * @param taskKeyB the task key bytes
   * @param metadata the metadata proto to store
   */
  private void storeTaskMetadata(Transaction tr, byte[] taskKeyB, TaskKeyMetadata metadata) {
    tr.set(taskKeys.pack(Tuple.from(taskKeyB, METADATA_KEY)), metadata.toByteArray());
  }

  /**
   * Stores a task in the unclaimed task space.
   *
   * @param tr          the transaction to use
   * @param visibleTime the time when the task should become visible
   * @param taskUuid    the task UUID
   * @param taskProto   the task proto to store
   */
  private void storeUnclaimedTask(Transaction tr, Instant visibleTime, UUID taskUuid, Task taskProto) {
    tr.set(unclaimedTasks.pack(Tuple.from(visibleTime.toEpochMilli(), taskUuid)), taskProto.toByteArray());
    // Always increment watch key to notify waiting workers
    incrementWatchKey(tr);
  }

  /**
   * Stores a task in the claimed task space.
   *
   * @param tr             the transaction to use
   * @param expirationTime the time when the claim expires
   * @param taskUuid       the task UUID
   * @param taskProto      the task proto to store
   */
  private void storeClaimedTask(Transaction tr, Instant expirationTime, UUID taskUuid, Task taskProto) {
    tr.set(claimedTasks.pack(Tuple.from(expirationTime.toEpochMilli(), taskUuid)), taskProto.toByteArray());
  }

  /**
   * Stores a task key with its version.
   *
   * @param tr       the transaction to use
   * @param taskKeyB the task key bytes
   * @param version  the version number
   * @param taskKey  the task key proto to store
   */
  private void storeTaskKey(Transaction tr, byte[] taskKeyB, long version, TaskKey taskKey) {
    tr.set(taskKeys.pack(Tuple.from(taskKeyB, version)), taskKey.toByteArray());
  }

  private TaskKey createNewTaskKeyProto(
      ByteString taskUuidBytes, T task, Duration ttl, Instant expectedExecutionTime) {
    return TaskKey.newBuilder()
        .setTaskUuid(taskUuidBytes)
        .setCreationTime(fromJavaTimestamp(config.getInstantSource().instant()))
        .setExpectedExecutionTime(fromJavaTimestamp(expectedExecutionTime))
        .setTtl(fromJavaDuration(ttl))
        .setThrottle(fromJavaDuration(config.getDefaultThrottle()))
        .setTask(config.getTaskSerializer().serialize(task))
        .build();
  }

  private String describeTask(UUID taskUuid, T task) {
    String name = task == null ? "null" : config.getTaskNameExtractor().apply(task);
    return "Task{uuid=" + taskUuid + ", name=" + name + "}";
  }

  static byte[] uuidToBytes(UUID uuid) {
    byte[] bytes = new byte[16];
    long msb = uuid.getMostSignificantBits();
    long lsb = uuid.getLeastSignificantBits();
    for (int i = 0; i < 8; i++) {
      bytes[i] = (byte) (msb >>> (8 * (7 - i)));
    }
    for (int i = 8; i < 16; i++) {
      bytes[i] = (byte) (lsb >>> (8 * (15 - i)));
    }
    return bytes;
  }

  static UUID bytesToUuid(byte[] bytes) {
    long msb = 0;
    for (int i = 0; i < 8; i++) {
      msb |= ((long) bytes[i] & 0xff) << (8 * (7 - i));
    }
    long lsb = 0;
    for (int i = 8; i < 16; i++) {
      lsb |= ((long) bytes[i] & 0xff) << (8 * (15 - i));
    }
    return new UUID(msb, lsb);
  }

  static com.google.protobuf.Duration fromJavaDuration(Duration duration) {
    return com.google.protobuf.Duration.newBuilder()
        .setSeconds(duration.getSeconds())
        .setNanos(duration.getNano())
        .build();
  }

  static Timestamp fromJavaTimestamp(Instant instant) {
    return Timestamp.newBuilder()
        .setSeconds(instant.getEpochSecond())
        .setNanos(instant.getNano())
        .build();
  }

  static Instant toJavaTimestamp(Timestamp timestamp) {
    return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
  }

  static Duration toJavaDuration(com.google.protobuf.Duration duration) {
    return Duration.ofSeconds(duration.getSeconds()).plusNanos(duration.getNanos());
  }
}
