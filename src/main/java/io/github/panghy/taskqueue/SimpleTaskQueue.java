package io.github.panghy.taskqueue;

import com.apple.foundationdb.Transaction;
import io.github.panghy.taskqueue.proto.TaskKeyMetadata;
import java.time.Duration;
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
}
