package io.github.panghy.taskqueue;

import com.apple.foundationdb.Transaction;
import io.github.panghy.taskqueue.proto.DeadLetteredTask;
import io.github.panghy.taskqueue.proto.TaskKeyMetadata;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * A simple task queue that does not support deduplication.
 *
 * @param <T> the type of task data
 */
public interface SimpleTaskQueue<T> extends BaseTaskQueue<UUID, T> {

  /**
   * Enqueues a task.
   *
   * @param task The task to enqueue.
   * @return A future that completes with the task metadata.
   */
  default CompletableFuture<TaskKeyMetadata> enqueue(T task) {
    return runAsync(tr -> enqueue(tr, task));
  }

  /**
   * Enqueues a task with a custom delay. Uses a standalone transaction.
   *
   * @param task  The task to enqueue.
   * @param delay The delay before the task should be executed.
   * @return A future that completes with the task metadata.
   */
  default CompletableFuture<TaskKeyMetadata> enqueue(T task, Duration delay) {
    return runAsync(tr -> enqueue(tr, task, delay));
  }

  /**
   * Enqueues a task with a custom delay and TTL. Uses a standalone transaction.
   *
   * @param task  The task to enqueue.
   * @param delay The delay before the task should be executed.
   * @param ttl   The time-to-live for the task.
   * @return A future that completes with the task metadata.
   */
  default CompletableFuture<TaskKeyMetadata> enqueue(T task, Duration delay, Duration ttl) {
    return runAsync(tr -> enqueue(tr, task, delay, ttl));
  }

  /**
   * Enqueues a task.
   *
   * @param tr   The transaction to use for the operation.
   * @param task The task to enqueue.
   * @return A future that completes with the task metadata.
   */
  default CompletableFuture<TaskKeyMetadata> enqueue(Transaction tr, T task) {
    return enqueue(tr, task, Duration.ZERO);
  }

  /**
   * Enqueues a task.
   *
   * @param tr    The transaction to use for the operation.
   * @param task  The task to enqueue.
   * @param delay The delay before the task should be executed.
   * @return A future that completes with the task metadata.
   */
  default CompletableFuture<TaskKeyMetadata> enqueue(Transaction tr, T task, Duration delay) {
    return enqueue(tr, task, delay, getConfig().getDefaultTtl());
  }

  /**
   * Enqueues a task.
   *
   * @param tr    The transaction to use for the operation.
   * @param task  The task to enqueue.
   * @param delay The delay before the task should be executed.
   * @param ttl   The time-to-live for the task.
   * @return A future that completes with the task metadata.
   */
  CompletableFuture<TaskKeyMetadata> enqueue(Transaction tr, T task, Duration delay, Duration ttl);

  // ---- Dead Letter Queue (DLQ) methods ----

  /**
   * Redrives up to {@code count} tasks from the dead letter queue back to the main queue.
   * Uses multiple standalone transactions, each processing up to {@code dlqRedriveBatchSize} tasks,
   * to avoid exceeding FoundationDB transaction limits for large redrives.
   *
   * @param count The maximum number of tasks to redrive.
   * @return A future that completes with the number of tasks actually redriven.
   */
  default CompletableFuture<Integer> redriveFromDlq(int count) {
    int batchSize = getConfig().getDlqRedriveBatchSize();
    java.util.concurrent.atomic.AtomicInteger totalRedriven = new java.util.concurrent.atomic.AtomicInteger(0);
    java.util.concurrent.atomic.AtomicInteger remaining = new java.util.concurrent.atomic.AtomicInteger(count);
    return com.apple.foundationdb.async.AsyncUtil.whileTrue(() -> {
          int thisBatch = Math.min(remaining.get(), batchSize);
          return runAsync(tr -> redriveFromDlq(tr, thisBatch)).thenApply(redriven -> {
            totalRedriven.addAndGet(redriven);
            remaining.addAndGet(-redriven);
            return redriven > 0 && remaining.get() > 0;
          });
        })
        .thenApply(v -> totalRedriven.get());
  }

  /**
   * Redrives up to {@code count} tasks from the dead letter queue back to the main queue.
   *
   * @param tr    The transaction to use for the operation.
   * @param count The maximum number of tasks to redrive.
   * @return A future that completes with the number of tasks actually redriven.
   */
  CompletableFuture<Integer> redriveFromDlq(Transaction tr, int count);

  /**
   * Purges all tasks from the dead letter queue.
   * Uses a standalone transaction.
   *
   * @return A future that completes when the DLQ has been purged.
   */
  default CompletableFuture<Void> purgeDlq() {
    return runAsync(this::purgeDlq);
  }

  /**
   * Purges all tasks from the dead letter queue.
   *
   * @param tr The transaction to use for the operation.
   * @return A future that completes when the DLQ has been purged.
   */
  CompletableFuture<Void> purgeDlq(Transaction tr);

  /**
   * Gets the number of tasks in the dead letter queue.
   * Uses a standalone transaction.
   *
   * @return A future that completes with the number of tasks in the DLQ.
   */
  default CompletableFuture<Long> getDlqSize() {
    return runAsync(this::getDlqSize);
  }

  /**
   * Gets the number of tasks in the dead letter queue.
   *
   * @param tr The transaction to use for the operation.
   * @return A future that completes with the number of tasks in the DLQ.
   */
  CompletableFuture<Long> getDlqSize(Transaction tr);

  /**
   * Lists tasks in the dead letter queue, ordered oldest first.
   * Uses a standalone transaction.
   *
   * @param limit The maximum number of tasks to return.
   * @return A future that completes with a list of dead-lettered tasks.
   */
  default CompletableFuture<List<DeadLetteredTask>> listDlqTasks(int limit) {
    return runAsync(tr -> listDlqTasks(tr, limit));
  }

  /**
   * Lists tasks in the dead letter queue, ordered oldest first.
   *
   * @param tr    The transaction to use for the operation.
   * @param limit The maximum number of tasks to return.
   * @return A future that completes with a list of dead-lettered tasks.
   */
  CompletableFuture<List<DeadLetteredTask>> listDlqTasks(Transaction tr, int limit);

  // ---- Queue depth counter methods ----

  /**
   * Gets the number of unclaimed tasks in the queue.
   * Uses a standalone transaction.
   *
   * @return A future that completes with the number of unclaimed tasks.
   */
  default CompletableFuture<Long> getUnclaimedCount() {
    return runAsync(this::getUnclaimedCount);
  }

  /**
   * Gets the number of unclaimed tasks in the queue.
   *
   * @param tr The transaction to use for the operation.
   * @return A future that completes with the number of unclaimed tasks.
   */
  CompletableFuture<Long> getUnclaimedCount(Transaction tr);

  /**
   * Gets the number of claimed (in-flight) tasks in the queue.
   * Uses a standalone transaction.
   *
   * @return A future that completes with the number of claimed tasks.
   */
  default CompletableFuture<Long> getClaimedCount() {
    return runAsync(this::getClaimedCount);
  }

  /**
   * Gets the number of claimed (in-flight) tasks in the queue.
   *
   * @param tr The transaction to use for the operation.
   * @return A future that completes with the number of claimed tasks.
   */
  CompletableFuture<Long> getClaimedCount(Transaction tr);

  /**
   * Gets the total queue depth (unclaimed + claimed tasks).
   * Uses a standalone transaction.
   *
   * @return A future that completes with the total queue depth.
   */
  default CompletableFuture<Long> getQueueDepth() {
    return runAsync(this::getQueueDepth);
  }

  /**
   * Gets the total queue depth (unclaimed + claimed tasks).
   *
   * @param tr The transaction to use for the operation.
   * @return A future that completes with the total queue depth.
   */
  CompletableFuture<Long> getQueueDepth(Transaction tr);
}
