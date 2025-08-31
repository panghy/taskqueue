package io.github.panghy.taskqueue;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import io.github.panghy.taskqueue.proto.TaskKeyMetadata;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public interface TaskQueue<K, T> {

  /**
   * Gets the configuration for this task queue.
   *
   * @return the configuration
   */
  TaskQueueConfig<K, T> getConfig();

  /**
   * Helper method to run a function within a transaction.
   *
   * @param function the function to run
   * @param <R>      the type of the result
   * @return a future that completes with the result of the function
   */
  default <R> CompletableFuture<R> runAsync(Function<Transaction, CompletableFuture<R>> function) {
    return getConfig().getDatabase().runAsync(function);
  }

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

  /**
   * Waits for a task to be available and claims it.
   *
   * @return a future that completes with the claimed task. This can be used to complete or fail the task with
   * {@link #completeTask} or {@link #failTask}.
   */
  default CompletableFuture<TaskClaim<K, T>> awaitAndClaimTask() {
    return awaitAndClaimTask(getConfig().getDatabase());
  }

  /**
   * Waits for a task to be available and claims it.
   *
   * @param db The database to use for the operation. This must be the actual database and not a transaction as we
   *           need to use a transaction to watch a key.
   * @return a future that completes with the claimed task. This can be used to complete or fail the task with
   * {@link #completeTask} or {@link #failTask}.
   */
  CompletableFuture<TaskClaim<K, T>> awaitAndClaimTask(Database db);

  /**
   * Completes the task. This will remove the task from the queue.
   *
   * @param taskClaim The task claim.
   * @return A future that completes when the task has been completed.
   */
  default CompletableFuture<Void> completeTask(TaskClaim<K, T> taskClaim) {
    return runAsync(tr -> completeTask(tr, taskClaim));
  }

  /**
   * Completes the task. This will remove the task from the queue.
   *
   * @param tr        The transaction to use for the operation.
   * @param taskClaim The task claim.
   * @return A future that completes when the task has been completed.
   */
  CompletableFuture<Void> completeTask(Transaction tr, TaskClaim<K, T> taskClaim);

  /**
   * Fails the task. This will cause the task to be retried at the current known highest version.
   *
   * @param taskClaim The task claim.
   * @return A future that completes when the task has been failed.
   */
  default CompletableFuture<Void> failTask(TaskClaim<K, T> taskClaim) {
    return runAsync(tr -> failTask(tr, taskClaim));
  }

  /**
   * Fails the task. This will cause the task to be retried at the current known highest version.
   *
   * @param tr        The transaction to use for the operation.
   * @param taskClaim The task claim.
   * @return A future that completes when the task has been failed.
   */
  CompletableFuture<Void> failTask(Transaction tr, TaskClaim<K, T> taskClaim);

  /**
   * Extends the TTL for a claimed task. This allows a worker to request more time to process a task.
   * If the task has expired but not been claimed by another worker, the TTL can still be extended.
   * If the task has been claimed by another worker (claim has changed), an exception will be thrown.
   *
   * @param taskClaim The task claim to extend.
   * @param extension The duration from now to set as the new expiration time. Must be positive.
   * @return A future that completes when the TTL has been extended.
   */
  default CompletableFuture<Void> extendTtl(TaskClaim<K, T> taskClaim, Duration extension) {
    return runAsync(tr -> extendTtl(tr, taskClaim, extension));
  }

  /**
   * Extends the TTL for a claimed task. This allows a worker to request more time to process a task.
   * If the task has expired but not been claimed by another worker, the TTL can still be extended.
   * If the task has been claimed by another worker (claim has changed), an exception will be thrown.
   *
   * @param tr        The transaction to use for the operation.
   * @param taskClaim The task claim to extend.
   * @param extension The duration from now to set as the new expiration time. Must be positive.
   * @return A future that completes when the TTL has been extended.
   */
  CompletableFuture<Void> extendTtl(Transaction tr, TaskClaim<K, T> taskClaim, Duration extension);

  /**
   * Checks whether the queue is empty.
   *
   * @return A future that completes with true if the queue is empty, false otherwise.
   */
  default CompletableFuture<Boolean> isEmpty() {
    return runAsync(this::isEmpty);
  }

  /**
   * Checks whether the queue is empty.
   *
   * @param tr The transaction to use for the operation.
   * @return A future that completes with true if the queue is empty, false otherwise.
   */
  CompletableFuture<Boolean> isEmpty(Transaction tr);
}
