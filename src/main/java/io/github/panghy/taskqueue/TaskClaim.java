package io.github.panghy.taskqueue;

import com.apple.foundationdb.Transaction;
import io.github.panghy.taskqueue.proto.Task;
import io.github.panghy.taskqueue.proto.TaskKey;
import io.github.panghy.taskqueue.proto.TaskKeyMetadata;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import lombok.Builder;

/**
 * Represents a claimed task with associated metadata and convenience methods for task operations.
 *
 * @param <K> the type of the task key
 * @param <T> the type of the task data
 */
@Builder
public record TaskClaim<K, T>(
    Task taskProto,
    TaskKey taskKeyProto,
    TaskKeyMetadata taskKeyMetadataProto,
    TaskQueue<K, T> taskQueue,
    K taskKey,
    T task) {

  /**
   * Completes the task, removing it from the queue.
   * Uses a standalone transaction.
   */
  public void complete() {
    taskQueue.completeTask(this);
  }

  /**
   * Completes the task within an existing transaction.
   *
   * @param tr the transaction to use
   */
  public void complete(Transaction tr) {
    taskQueue.completeTask(tr, this);
  }

  /**
   * Fails the task, causing it to be retried.
   * Uses a standalone transaction.
   */
  public void fail() {
    taskQueue.failTask(this);
  }

  /**
   * Fails the task within an existing transaction.
   *
   * @param tr the transaction to use
   */
  public void fail(Transaction tr) {
    taskQueue.failTask(tr, this);
  }

  /**
   * Extends the TTL (time-to-live) for this task claim.
   * Uses a standalone transaction.
   *
   * @param extension the duration from now to set as the new expiration time (must be positive)
   */
  public void extend(Duration extension) {
    taskQueue.extendTtl(this, extension);
  }

  /**
   * Extends the TTL within an existing transaction.
   *
   * @param tr        the transaction to use
   * @param extension the duration from now to set as the new expiration time (must be positive)
   */
  public void extend(Transaction tr, Duration extension) {
    taskQueue.extendTtl(tr, this, extension);
  }

  /**
   * Gets the number of attempts made on this task.
   *
   * @return the attempt count (includes the current attempt)
   */
  public long getAttempts() {
    return taskProto.getAttempts();
  }

  /**
   * Gets the task version number.
   * Higher versions supersede lower versions for the same task key.
   *
   * @return the task version
   */
  public long getTaskVersion() {
    return taskProto.getTaskVersion();
  }

  /**
   * Gets the unique identifier for this task instance.
   *
   * @return the task UUID
   */
  public UUID getTaskUuid() {
    return KeyedTaskQueue.bytesToUuid(taskProto.getTaskUuid().toByteArray());
  }

  /**
   * Gets the unique identifier for this claim.
   *
   * @return the claim UUID
   */
  public UUID getClaimUuid() {
    return KeyedTaskQueue.bytesToUuid(taskProto.getClaim().toByteArray());
  }

  /**
   * Gets the creation time of this task.
   *
   * @return the instant when the task was created
   */
  public Instant getCreationTime() {
    return KeyedTaskQueue.toJavaTimestamp(taskProto.getCreationTime());
  }

  /**
   * Gets the expected execution time for this task.
   *
   * @return the instant when the task was expected to execute
   */
  public Instant getExpectedExecutionTime() {
    return KeyedTaskQueue.toJavaTimestamp(taskKeyProto.getExpectedExecutionTime());
  }

  /**
   * Gets the expiration time for this claim.
   *
   * @return the instant when this claim expires
   */
  public Instant getExpirationTime() {
    return KeyedTaskQueue.toJavaTimestamp(
        taskKeyMetadataProto.getCurrentClaim().getExpirationTime());
  }

  /**
   * Gets the TTL (time-to-live) duration for this task.
   *
   * @return the TTL duration
   */
  public Duration getTtl() {
    return KeyedTaskQueue.toJavaDuration(taskKeyProto.getTtl());
  }

  /**
   * Gets the throttle duration for this task.
   * This is the minimum time between execution attempts.
   *
   * @return the throttle duration
   */
  public Duration getThrottle() {
    return KeyedTaskQueue.toJavaDuration(taskKeyProto.getThrottle());
  }

  /**
   * Gets the time remaining before this claim expires.
   *
   * @return the duration until expiration, or Duration.ZERO if already expired
   */
  public Duration getTimeUntilExpiration() {
    Instant now = taskQueue.getConfig().getInstantSource().instant();
    Duration remaining = Duration.between(now, getExpirationTime());
    return remaining.isNegative() ? Duration.ZERO : remaining;
  }

  /**
   * Checks if this claim has expired.
   *
   * @return true if the claim has expired
   */
  public boolean isExpired() {
    return taskQueue.getConfig().getInstantSource().instant().isAfter(getExpirationTime());
  }
}
