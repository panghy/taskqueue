package io.github.panghy.taskqueue;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * A collection of task queue implementations.
 */
public abstract class TaskQueues {

  private TaskQueues() {}

  /**
   * Creates a keyed task queue. This supports deduplication of tasks based on a key.
   *
   * @param config the configuration for the task queue
   * @param <K>    the type of the task key
   * @param <T>    the type of the task data
   * @return a future that completes with the task queue
   */
  public static <K, T> CompletableFuture<TaskQueue<K, T>> createTaskQueue(TaskQueueConfig<K, T> config) {
    return config.getDatabase().runAsync(tr -> KeyedTaskQueue.createOrOpen(config, tr));
  }

  /**
   * Creates a simple task queue. This does not support deduplication of tasks.
   *
   * @param config the configuration for the task queue
   * @param <T>    the type of the task data
   * @return a future that completes with the task queue
   */
  public static <T> CompletableFuture<SimpleTaskQueue<T>> createSimpleTaskQueue(TaskQueueConfig<UUID, T> config) {
    return config.getDatabase().runAsync(tr -> SimpleTaskQueueWrapper.createOrOpen(config, tr));
  }
}
