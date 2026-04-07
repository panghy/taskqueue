package io.github.panghy.taskqueue;

import java.util.UUID;

/**
 * Listener interface for task queue lifecycle events.
 * All methods have default no-op implementations, so it is safe to invoke
 * without null checks and consumers can override only the callbacks they need.
 *
 * @param <K> the type of task keys
 * @param <T> the type of task data
 */
public interface TaskQueueListener<K, T> {

  /**
   * Called when a task has exhausted all retry attempts and is being permanently removed.
   *
   * @param taskKey  the task key
   * @param task     the task data
   * @param attempts the total number of attempts made
   */
  default void onTaskExhausted(K taskKey, T task, long attempts) {}

  /**
   * Called when a claimed task's TTL has expired and it is being reclaimed.
   *
   * @param taskKey   the task key
   * @param task      the task data
   * @param claimUuid the UUID of the expired claim
   */
  default void onTaskExpired(K taskKey, T task, UUID claimUuid) {}

  /**
   * Called when an expired task is picked up by a new worker.
   *
   * @param taskKey  the task key
   * @param task     the task data
   * @param attempts the attempt number for this new claim
   */
  default void onTaskReclaimed(K taskKey, T task, long attempts) {}
}
