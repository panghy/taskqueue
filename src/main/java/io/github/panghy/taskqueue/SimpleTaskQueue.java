package io.github.panghy.taskqueue;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import io.github.panghy.taskqueue.proto.DeadLetteredTask;
import io.github.panghy.taskqueue.proto.TaskKeyMetadata;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * A simple task queue that does not support deduplication.
 *
 * @param <T> the type of task data
 */
public interface SimpleTaskQueue<T> {

  /**
   * Gets the configuration for this task queue.
   *
   * @return the configuration
   */
  TaskQueueConfig<UUID, T> getConfig();

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
   * Enqueues a task.
   *
   * @param task The task to enqueue.
   * @return A future that completes with the task metadata.
   */
  default CompletableFuture<TaskKeyMetadata> enqueue(T task) {
    return runAsync(tr -> enqueue(tr, task));
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

  /**
   * Waits for a task to be available and claims it.
   *
   * @return a future that completes with the claimed task. This can be used to complete or fail the task with
   * {@link #completeTask} or {@link #failTask}.
   */
  default CompletableFuture<TaskClaim<UUID, T>> awaitAndClaimTask() {
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
  CompletableFuture<TaskClaim<UUID, T>> awaitAndClaimTask(Database db);

  /**
   * Completes the task. This will remove the task from the queue.
   *
   * @param taskClaim The task claim.
   * @return A future that completes when the task has been completed.
   */
  default CompletableFuture<Void> completeTask(TaskClaim<UUID, T> taskClaim) {
    return runAsync(tr -> completeTask(tr, taskClaim));
  }

  /**
   * Completes the task. This will remove the task from the queue.
   *
   * @param tr        The transaction to use for the operation.
   * @param taskClaim The task claim.
   * @return A future that completes when the task has been completed.
   */
  CompletableFuture<Void> completeTask(Transaction tr, TaskClaim<UUID, T> taskClaim);

  /**
   * Fails the task. This will cause the task to be retried at the current known highest version.
   * Uses a standalone transaction.
   *
   * @param taskClaim The task claim.
   * @return A future that completes when the task has been failed.
   */
  default CompletableFuture<Void> failTask(TaskClaim<UUID, T> taskClaim) {
    return runAsync(tr -> failTask(tr, taskClaim, null));
  }

  /**
   * Fails the task with a failure reason. This will cause the task to be retried at the current known highest version.
   * Uses a standalone transaction.
   *
   * @param taskClaim     The task claim.
   * @param failureReason The reason for the failure (may be null).
   * @return A future that completes when the task has been failed.
   */
  default CompletableFuture<Void> failTask(TaskClaim<UUID, T> taskClaim, String failureReason) {
    return runAsync(tr -> failTask(tr, taskClaim, failureReason));
  }

  /**
   * Fails the task within an existing transaction. This will cause the task to be retried at the current known
   * highest version.
   *
   * @param tr        The transaction to use for the operation.
   * @param taskClaim The task claim.
   * @return A future that completes when the task has been failed.
   */
  default CompletableFuture<Void> failTask(Transaction tr, TaskClaim<UUID, T> taskClaim) {
    return failTask(tr, taskClaim, null);
  }

  /**
   * Fails the task within an existing transaction with a failure reason. This will cause the task to be retried
   * at the current known highest version.
   *
   * @param tr            The transaction to use for the operation.
   * @param taskClaim     The task claim.
   * @param failureReason The reason for the failure (may be null).
   * @return A future that completes when the task has been failed.
   */
  CompletableFuture<Void> failTask(Transaction tr, TaskClaim<UUID, T> taskClaim, String failureReason);

  /**
   * Extends the TTL for a claimed task. This allows a worker to request more time to process a task.
   * If the task has expired but not been claimed by another worker, the TTL can still be extended.
   * If the task has been claimed by another worker (claim has changed), an exception will be thrown.
   *
   * @param taskClaim The task claim to extend.
   * @param extension The duration from now to set as the new expiration time. Must be positive.
   * @return A future that completes when the TTL has been extended.
   */
  default CompletableFuture<Void> extendTtl(TaskClaim<UUID, T> taskClaim, Duration extension) {
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
  CompletableFuture<Void> extendTtl(Transaction tr, TaskClaim<UUID, T> taskClaim, Duration extension);

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

  /**
   * Checks whether the queue has any visible unclaimed tasks ready to be processed.
   * This only returns true if there are unclaimed tasks whose visibility time has passed.
   * Tasks that are claimed or scheduled for future execution are not counted.
   *
   * @return A future that completes with true if there are visible unclaimed tasks, false otherwise.
   */
  default CompletableFuture<Boolean> hasVisibleUnclaimedTasks() {
    return runAsync(this::hasVisibleUnclaimedTasks);
  }

  /**
   * Checks whether the queue has any visible unclaimed tasks ready to be processed.
   * This only returns true if there are unclaimed tasks whose visibility time has passed.
   * Tasks that are claimed or scheduled for future execution are not counted.
   *
   * @param tr The transaction to use for the operation.
   * @return A future that completes with true if there are visible unclaimed tasks, false otherwise.
   */
  CompletableFuture<Boolean> hasVisibleUnclaimedTasks(Transaction tr);

  /**
   * Checks whether the queue has any claimed tasks.
   * This returns true if there are any tasks that are currently being processed by workers.
   *
   * @return A future that completes with true if there are claimed tasks, false otherwise.
   */
  default CompletableFuture<Boolean> hasClaimedTasks() {
    return runAsync(this::hasClaimedTasks);
  }

  /**
   * Checks whether the queue has any claimed tasks.
   * This returns true if there are any tasks that are currently being processed by workers.
   *
   * @param tr The transaction to use for the operation.
   * @return A future that completes with true if there are claimed tasks, false otherwise.
   */
  CompletableFuture<Boolean> hasClaimedTasks(Transaction tr);

  /**
   * Waits for the queue to become completely empty (no unclaimed or claimed tasks).
   * This method returns a future that completes when there are no tasks in the queue.
   * It uses a watch similar to awaitAndClaimTask to be notified when the queue state changes.
   *
   * @return A future that completes when the queue is empty.
   */
  default CompletableFuture<Void> awaitQueueEmpty() {
    return awaitQueueEmpty(getConfig().getDatabase());
  }

  /**
   * Waits for the queue to become completely empty (no unclaimed or claimed tasks).
   * This method returns a future that completes when there are no tasks in the queue.
   * It uses a watch similar to awaitAndClaimTask to be notified when the queue state changes.
   *
   * @param db The database to use for the operation. This must be the actual database and not a transaction as we
   *           need to use a transaction to watch a key.
   * @return A future that completes when the queue is empty.
   */
  CompletableFuture<Void> awaitQueueEmpty(Database db);

  // ---- Dead Letter Queue (DLQ) methods ----

  /**
   * Redrives a specific task from the dead letter queue back to the main queue by task key.
   * Uses a standalone transaction.
   *
   * @param taskKey The key of the task to redrive.
   * @return A future that completes when the task has been redriven.
   */
  default CompletableFuture<Void> redriveFromDlq(UUID taskKey) {
    return runAsync(tr -> redriveFromDlq(tr, taskKey));
  }

  /**
   * Redrives a specific task from the dead letter queue back to the main queue by task key.
   *
   * @param tr      The transaction to use for the operation.
   * @param taskKey The key of the task to redrive.
   * @return A future that completes when the task has been redriven.
   */
  CompletableFuture<Void> redriveFromDlq(Transaction tr, UUID taskKey);

  /**
   * Redrives up to {@code count} tasks from the dead letter queue back to the main queue.
   * Uses a standalone transaction.
   *
   * @param count The maximum number of tasks to redrive.
   * @return A future that completes with the number of tasks actually redriven.
   */
  default CompletableFuture<Integer> redriveFromDlq(int count) {
    return runAsync(tr -> redriveFromDlq(tr, count));
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
}
