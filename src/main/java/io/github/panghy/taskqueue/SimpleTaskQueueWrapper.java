package io.github.panghy.taskqueue;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.TransactionContext;
import io.github.panghy.taskqueue.proto.DeadLetteredTask;
import io.github.panghy.taskqueue.proto.TaskKeyMetadata;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * A simple task queue that does not support deduplication. This is a wrapper around KeyedTaskQueue that generates a
 * random UUID for each task.
 *
 * @param <T> the type of task data
 */
public class SimpleTaskQueueWrapper<T> implements SimpleTaskQueue<T> {

  private final TaskQueue<UUID, T> taskQueue;

  SimpleTaskQueueWrapper(TaskQueue<UUID, T> taskQueue) {
    this.taskQueue = taskQueue;
  }

  static <T> CompletableFuture<SimpleTaskQueue<T>> createOrOpen(
      TaskQueueConfig<UUID, T> config, TransactionContext context) {
    return KeyedTaskQueue.createOrOpen(config, context).thenApply(SimpleTaskQueueWrapper::new);
  }

  @Override
  public TaskQueueConfig<UUID, T> getConfig() {
    return taskQueue.getConfig();
  }

  @Override
  public CompletableFuture<TaskKeyMetadata> enqueue(Transaction tr, T task, Duration delay, Duration ttl) {
    return taskQueue.enqueue(tr, UUID.randomUUID(), task, delay, ttl);
  }

  @Override
  public CompletableFuture<TaskClaim<UUID, T>> awaitAndClaimTask(Database db) {
    return taskQueue.awaitAndClaimTask(db);
  }

  @Override
  public CompletableFuture<Void> completeTask(Transaction tr, TaskClaim<UUID, T> taskClaim) {
    return taskQueue.completeTask(tr, taskClaim);
  }

  @Override
  public CompletableFuture<Void> failTask(Transaction tr, TaskClaim<UUID, T> taskClaim, String failureReason) {
    return taskQueue.failTask(tr, taskClaim, failureReason);
  }

  @Override
  public CompletableFuture<Void> extendTtl(Transaction tr, TaskClaim<UUID, T> taskClaim, Duration extension) {
    return taskQueue.extendTtl(tr, taskClaim, extension);
  }

  @Override
  public CompletableFuture<Boolean> isEmpty(Transaction tr) {
    return taskQueue.isEmpty(tr);
  }

  @Override
  public CompletableFuture<Boolean> hasVisibleUnclaimedTasks(Transaction tr) {
    return taskQueue.hasVisibleUnclaimedTasks(tr);
  }

  @Override
  public CompletableFuture<Boolean> hasClaimedTasks(Transaction tr) {
    return taskQueue.hasClaimedTasks(tr);
  }

  @Override
  public CompletableFuture<Void> awaitQueueEmpty(Database db) {
    return taskQueue.awaitQueueEmpty(db);
  }

  @Override
  public CompletableFuture<Integer> redriveFromDlq(Transaction tr, int count) {
    return taskQueue.redriveFromDlq(tr, count);
  }

  @Override
  public CompletableFuture<Void> purgeDlq(Transaction tr) {
    return taskQueue.purgeDlq(tr);
  }

  @Override
  public CompletableFuture<Long> getDlqSize(Transaction tr) {
    return taskQueue.getDlqSize(tr);
  }

  @Override
  public CompletableFuture<List<DeadLetteredTask>> listDlqTasks(Transaction tr, int limit) {
    return taskQueue.listDlqTasks(tr, limit);
  }
}
