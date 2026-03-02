package io.github.panghy.taskqueue;

import com.apple.foundationdb.Transaction;
import io.github.panghy.taskqueue.proto.TaskKeyMetadata;
import java.time.Duration;
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
}
