package io.github.panghy.taskqueue;

import com.apple.foundationdb.Transaction;
import io.github.panghy.taskqueue.proto.DeadLetteredTask;
import io.github.panghy.taskqueue.proto.TaskKeyMetadata;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface TaskQueue<K, T> extends BaseTaskQueue<K, T> {

  /**
   * Enqueues a task if it does not already exist. Existence is determined by the task key. Uses a standalone
   * transaction.
   *
   * @param taskKey the key for the task
   * @param task    the task to enqueue
   * @return a future that completes with the task metadata
   */
  default CompletableFuture<TaskKeyMetadata> enqueueIfNotExists(K taskKey, T task) {
    return runAsync(tr ->
        enqueueIfNotExists(tr, taskKey, task, Duration.ZERO, getConfig().getDefaultTtl(), false));
  }

  /**
   * Enqueues a task if it does not already exist. Existence is determined by the task key.
   *
   * @param tr      the transaction to use for the operation
   * @param taskKey the key for the task
   * @param task    the task to enqueue
   * @return a future that completes with the task metadata
   */
  default CompletableFuture<TaskKeyMetadata> enqueueIfNotExists(Transaction tr, K taskKey, T task) {
    return enqueueIfNotExists(tr, taskKey, task, Duration.ZERO, getConfig().getDefaultTtl(), false);
  }

  /**
   * Enqueues a task if it does not already exist. Existence is determined by the task key.
   *
   * @param tr      the transaction to use for the operation
   * @param taskKey the key for the task
   * @param task    the task to enqueue
   * @param delay   the delay before the task should be executed
   * @return a future that completes with the task metadata
   */
  default CompletableFuture<TaskKeyMetadata> enqueueIfNotExists(Transaction tr, K taskKey, T task, Duration delay) {
    return enqueueIfNotExists(tr, taskKey, task, delay, getConfig().getDefaultTtl(), false);
  }

  /**
   * Enqueues a task if it does not already exist. Existence is determined by the task key.
   *
   * @param tr      the transaction to use for the operation
   * @param taskKey the key for the task
   * @param task    the task to enqueue
   * @param delay   the delay before the task should be executed
   * @param ttl     the time-to-live for the task
   * @return a future that completes with the task metadata
   */
  default CompletableFuture<TaskKeyMetadata> enqueueIfNotExists(
      Transaction tr, K taskKey, T task, Duration delay, Duration ttl) {
    return enqueueIfNotExists(tr, taskKey, task, delay, ttl, false);
  }

  /**
   * Enqueues a task if it does not already exist. Existence is determined by the task key. If the task has been
   * submitted, enqueueIfAlreadyRunning determines whether a new task would be created if the task is already running.
   *
   * @param taskKey                 The key for the task.
   * @param task                    The task to enqueue.
   * @param delay                   The delay before the task should be executed
   * @param ttl                     The time-to-live for the task.
   * @param enqueueIfAlreadyRunning Whether to enqueue a new task if the task is already running.
   * @return A future that completes with the task metadata.
   */
  default CompletableFuture<TaskKeyMetadata> enqueueIfNotExists(
      K taskKey, T task, Duration delay, Duration ttl, boolean enqueueIfAlreadyRunning) {
    return runAsync(tr -> enqueueIfNotExists(tr, taskKey, task, delay, ttl, enqueueIfAlreadyRunning));
  }

  /**
   * Enqueues a task if it does not already exist. Existence is determined by the task key. If the task has been
   * submitted, enqueueIfAlreadyRunning determines whether a new task would be created if the task is already running.
   *
   * @param tr                      The transaction to use for the operation.
   * @param taskKey                 The key for the task.
   * @param task                    The task to enqueue.
   * @param delay                   The delay before the task should be executed
   * @param ttl                     The time-to-live for the task.
   * @param enqueueIfAlreadyRunning Whether to enqueue a new task if the task is already running.
   * @return A future that completes with the task metadata.
   */
  CompletableFuture<TaskKeyMetadata> enqueueIfNotExists(
      Transaction tr, K taskKey, T task, Duration delay, Duration ttl, boolean enqueueIfAlreadyRunning);

  /**
   * Enqueues a task. If a task with the same key exists (whether it is running or not), a new version of the task
   * will be created (and will be picked up when the existing task completes).
   *
   * @param taskKey The key for the task.
   * @param task    The task to enqueue.
   * @return A future that completes with the task metadata.
   */
  default CompletableFuture<TaskKeyMetadata> enqueue(K taskKey, T task) {
    return runAsync(tr -> enqueue(tr, taskKey, task));
  }

  /**
   * Enqueues a task with a custom delay. Uses a standalone transaction. If a task with the same key exists
   * (whether it is running or not), a new version of the task will be created (and will be picked up when the
   * existing task completes).
   *
   * @param taskKey The key for the task.
   * @param task    The task to enqueue.
   * @param delay   The delay before the task should be executed.
   * @return A future that completes with the task metadata.
   */
  default CompletableFuture<TaskKeyMetadata> enqueue(K taskKey, T task, Duration delay) {
    return runAsync(tr -> enqueue(tr, taskKey, task, delay));
  }

  /**
   * Enqueues a task with a custom delay and TTL. Uses a standalone transaction. If a task with the same key exists
   * (whether it is running or not), a new version of the task will be created (and will be picked up when the
   * existing task completes).
   *
   * @param taskKey The key for the task.
   * @param task    The task to enqueue.
   * @param delay   The delay before the task should be executed.
   * @param ttl     The time-to-live for the task.
   * @return A future that completes with the task metadata.
   */
  default CompletableFuture<TaskKeyMetadata> enqueue(K taskKey, T task, Duration delay, Duration ttl) {
    return runAsync(tr -> enqueue(tr, taskKey, task, delay, ttl));
  }

  /**
   * Enqueues a task. If a task with the same key exists (whether it is running or not), a new version of the task
   * will be created (and will be picked up when the existing task completes).
   *
   * @param tr      The transaction to use for the operation.
   * @param taskKey The key for the task.
   * @param task    The task to enqueue.
   * @return A future that completes with the task metadata.
   */
  default CompletableFuture<TaskKeyMetadata> enqueue(Transaction tr, K taskKey, T task) {
    return enqueue(tr, taskKey, task, Duration.ZERO);
  }

  /**
   * Enqueues a task. If a task with the same key exists (whether it is running or not), a new version of the task
   * will be created (and will be picked up when the existing task completes).
   *
   * @param tr      The transaction to use for the operation.
   * @param taskKey The key for the task.
   * @param task    The task to enqueue.
   * @param delay   The delay before the task should be executed.
   * @return A future that completes with the task metadata.
   */
  default CompletableFuture<TaskKeyMetadata> enqueue(Transaction tr, K taskKey, T task, Duration delay) {
    return enqueue(tr, taskKey, task, delay, getConfig().getDefaultTtl());
  }

  /**
   * Enqueues a task. If a task with the same key exists (whether it is running or not), a new version of the task
   * will be created (and will be picked up when the existing task completes).
   *
   * @param tr      The transaction to use for the operation.
   * @param taskKey The key for the task.
   * @param task    The task to enqueue.
   * @param delay   The delay before the task should be executed.
   * @param ttl     The time-to-live for the task.
   * @return A future that completes with the task metadata.
   */
  CompletableFuture<TaskKeyMetadata> enqueue(Transaction tr, K taskKey, T task, Duration delay, Duration ttl);

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
