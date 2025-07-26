package io.github.panghy.taskqueue;

import com.apple.foundationdb.tuple.Tuple;

import java.time.Duration;
import java.util.Objects;

/**
 * Configuration for a TaskQueue instance with type parameters for task key and task data.
 *
 * @param <K> Type of the task key
 * @param <T> Type of the task data
 */
public class TaskQueueConfig<K, T> {
  private final Tuple keyPrefix;
  private final Duration defaultTtl;
  private final int maxAttempts;
  private final Duration defaultThrottle;
  private final TaskSerializer<K> keySerializer;
  private final TaskSerializer<T> taskSerializer;

  private TaskQueueConfig(Builder<K, T> builder) {
    this.keyPrefix = Objects.requireNonNull(builder.keyPrefix, "keyPrefix must not be null");
    this.defaultTtl = Objects.requireNonNull(builder.defaultTtl, "defaultTtl must not be null");
    this.maxAttempts = builder.maxAttempts;
    this.defaultThrottle = Objects.requireNonNull(builder.defaultThrottle, "defaultThrottle must not be null");
    this.keySerializer = Objects.requireNonNull(builder.keySerializer, "keySerializer must not be null");
    this.taskSerializer = Objects.requireNonNull(builder.taskSerializer, "taskSerializer must not be null");

    if (maxAttempts <= 0) {
      throw new IllegalArgumentException("maxAttempts must be positive");
    }
    if (defaultTtl.isNegative() || defaultTtl.isZero()) {
      throw new IllegalArgumentException("defaultTtl must be positive");
    }
    if (defaultThrottle.isNegative()) {
      throw new IllegalArgumentException("defaultThrottle must not be negative");
    }
  }

  /**
   * Gets the FoundationDB key prefix tuple for this task queue.
   * All keys for this queue will be prefixed with this tuple.
   *
   * @return the key prefix tuple
   */
  public Tuple getKeyPrefix() {
    return keyPrefix;
  }

  /**
   * Gets the default time-to-live for tasks in this queue.
   * This determines how long a worker has to process a task before it
   * becomes available for other workers to claim.
   *
   * @return the default TTL duration
   */
  public Duration getDefaultTtl() {
    return defaultTtl;
  }

  /**
   * Gets the maximum number of attempts allowed for a task.
   * After this many failed attempts, the task will not be retried.
   *
   * @return the maximum number of attempts
   */
  public int getMaxAttempts() {
    return maxAttempts;
  }

  /**
   * Gets the default throttle duration for tasks with the same key.
   * This controls the minimum time between executions of tasks with
   * identical task keys, preventing rapid re-execution.
   *
   * @return the default throttle duration
   */
  public Duration getDefaultThrottle() {
    return defaultThrottle;
  }

  /**
   * Gets the serializer for task keys.
   * This is used to convert task key objects to/from byte arrays for storage.
   *
   * @return the task key serializer
   */
  public TaskSerializer<K> getKeySerializer() {
    return keySerializer;
  }

  /**
   * Gets the serializer for task data.
   * This is used to convert task data objects to/from byte arrays for storage.
   *
   * @return the task data serializer
   */
  public TaskSerializer<T> getTaskSerializer() {
    return taskSerializer;
  }

  /**
   * Creates a new builder for TaskQueueConfig.
   *
   * @param <K> the type of task keys
   * @param <T> the type of task data
   * @return a new builder instance
   */
  public static <K, T> Builder<K, T> builder() {
    return new Builder<>();
  }

  /**
   * Serializer interface for converting between objects and byte arrays.
   * Implementations must handle null values appropriately and ensure
   * round-trip serialization/deserialization produces equivalent objects.
   *
   * @param <V> the type of value to serialize
   */
  public interface TaskSerializer<V> {
    /**
     * Serializes a value to a byte array for storage in FoundationDB.
     *
     * @param value the value to serialize (may be null)
     * @return the serialized byte array
     * @throws IllegalArgumentException if the value cannot be serialized
     */
    byte[] serialize(V value);

    /**
     * Deserializes a byte array back to the original value type.
     *
     * @param bytes the byte array to deserialize
     * @return the deserialized value (may be null)
     * @throws IllegalArgumentException if the bytes cannot be deserialized
     */
    V deserialize(byte[] bytes);
  }

  public static class Builder<K, T> {
    private Tuple keyPrefix;
    private Duration defaultTtl = Duration.ofMinutes(5);
    private int maxAttempts = 3;
    private Duration defaultThrottle = Duration.ofSeconds(1);
    private TaskSerializer<K> keySerializer;
    private TaskSerializer<T> taskSerializer;

    private Builder() {
    }

    /**
     * Sets the key prefix for the task queue.
     */
    public Builder<K, T> keyPrefix(Tuple keyPrefix) {
      this.keyPrefix = keyPrefix;
      return this;
    }

    /**
     * Sets the default TTL (time-to-live) for tasks.
     * Default: 5 minutes
     */
    public Builder<K, T> defaultTtl(Duration defaultTtl) {
      this.defaultTtl = defaultTtl;
      return this;
    }

    /**
     * Sets the maximum number of attempts for a task before it's considered failed.
     * Default: 3
     */
    public Builder<K, T> maxAttempts(int maxAttempts) {
      this.maxAttempts = maxAttempts;
      return this;
    }

    /**
     * Sets the default throttle duration between task executions for the same key.
     * Default: 1 second
     */
    public Builder<K, T> defaultThrottle(Duration defaultThrottle) {
      this.defaultThrottle = defaultThrottle;
      return this;
    }

    /**
     * Sets the serializer for task keys.
     */
    public Builder<K, T> keySerializer(TaskSerializer<K> keySerializer) {
      this.keySerializer = keySerializer;
      return this;
    }

    /**
     * Sets the serializer for task data.
     */
    public Builder<K, T> taskSerializer(TaskSerializer<T> taskSerializer) {
      this.taskSerializer = taskSerializer;
      return this;
    }

    public TaskQueueConfig<K, T> build() {
      return new TaskQueueConfig<>(this);
    }
  }
}
