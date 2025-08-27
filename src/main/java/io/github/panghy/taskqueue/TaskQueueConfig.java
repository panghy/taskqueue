package io.github.panghy.taskqueue;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.directory.Directory;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.time.InstantSource;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;
import lombok.Getter;

/**
 * Configuration for a TaskQueue instance with type parameters for task key and task data.
 *
 * @param <K> Type of the task key
 * @param <T> Type of the task data
 */
public class TaskQueueConfig<K, T> {

  /**
   * -- GETTER --
   * Gets the FoundationDB database for this task queue.
   * This is used to perform all database operations.
   */
  @Getter
  private final Database database;

  /**
   * -- GETTER --
   * Gets the FoundationDB directory for this task queue.
   * Data for this queue will be stored within this directory.
   */
  @Getter
  private final Directory directory;
  /**
   * Gets the default time-to-live for tasks in this queue.
   * This determines how long a worker has to process a task before it
   * becomes available for other workers to claim.
   */
  @Getter
  private final Duration defaultTtl;
  /**
   * Gets the maximum number of attempts allowed for a task.
   * After this many failed attempts, the task will not be retried.
   */
  @Getter
  private final int maxAttempts;
  /**
   * Gets the default throttle duration for tasks with the same key.
   * This controls the minimum time between executions of tasks with
   * identical task keys, preventing rapid re-execution.
   */
  @Getter
  private final Duration defaultThrottle;
  /**
   * -- GETTER --
   * Gets the serializer for task keys.
   * This is used to convert task key objects to/from byte arrays for storage.
   */
  @Getter
  private final TaskSerializer<K> keySerializer;
  /**
   * -- GETTER --
   * Gets the serializer for task data.
   * This is used to convert task data objects to/from byte arrays for storage.
   */
  @Getter
  private final TaskSerializer<T> taskSerializer;
  /**
   * -- GETTER --
   * Gets the function to extract a name from a task.
   * This is used to generate a human-readable name for tasks.
   */
  @Getter
  private final Function<T, String> taskNameExtractor;
  /**
   * -- GETTER --
   * Gets the estimated number of workers that will be processing tasks from this queue.
   * This is used to determine the number of parallel scans to perform when looking for tasks.
   */
  @Getter
  private final int estimatedWorkerCount;
  /**
   * -- GETTER --
   * Gets the instant source for this queue.
   * This is used to determine the current time for task expiration and other time-based operations.
   */
  @Getter
  private final InstantSource instantSource;

  private TaskQueueConfig(Builder<K, T> builder) {
    this.database = Objects.requireNonNull(builder.database, "database must not be null");
    this.directory = Objects.requireNonNull(builder.directory, "subspace must not be null");
    this.defaultTtl = Objects.requireNonNull(builder.defaultTtl, "defaultTtl must not be null");
    this.maxAttempts = builder.maxAttempts;
    this.defaultThrottle = Objects.requireNonNull(builder.defaultThrottle, "defaultThrottle must not be null");
    this.keySerializer = Objects.requireNonNull(builder.keySerializer, "keySerializer must not be null");
    this.taskSerializer = Objects.requireNonNull(builder.taskSerializer, "taskSerializer must not be null");
    this.taskNameExtractor =
        Objects.requireNonNull(builder.taskNameExtractor, "taskNameExtractor must not be null");
    this.estimatedWorkerCount = builder.estimatedWorkerCount;
    this.instantSource = Objects.requireNonNull(builder.instantSource, "instantSource must not be null");

    if (maxAttempts <= 0) {
      throw new IllegalArgumentException("maxAttempts must be positive");
    }
    if (defaultTtl.isNegative() || defaultTtl.isZero()) {
      throw new IllegalArgumentException("defaultTtl must be positive");
    }
    if (defaultThrottle.isNegative()) {
      throw new IllegalArgumentException("defaultThrottle must not be negative");
    }
    if (estimatedWorkerCount <= 0) {
      throw new IllegalArgumentException("estimatedWorkerCount must be positive");
    }
  }

  /**
   * Creates a new builder for TaskQueueConfig with auto-generated {@link UUID} keys.
   *
   * @param <T> the type of task data
   * @return a new builder instance
   */
  public static <T> Builder<UUID, T> builder(
      Database database, Directory directory, TaskSerializer<T> taskSerializer) {
    return new Builder<>(database, directory, new UUIDSerializer(), taskSerializer);
  }

  /**
   * Creates a new builder for TaskQueueConfig.
   *
   * @param <K> the type of task keys
   * @param <T> the type of task data
   * @return a new builder instance
   */
  public static <K, T> Builder<K, T> builder(
      Database database, Directory directory, TaskSerializer<K> keySerializer, TaskSerializer<T> taskSerializer) {
    return new Builder<>(database, directory, keySerializer, taskSerializer);
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
    ByteString serialize(V value);

    /**
     * Deserializes a byte array back to the original value type.
     *
     * @param bytes the byte array to deserialize
     * @return the deserialized value (may be null)
     * @throws IllegalArgumentException if the bytes cannot be deserialized
     */
    V deserialize(ByteString bytes);
  }

  public static class Builder<K, T> {
    public Function<T, String> taskNameExtractor = Object::toString;
    public int estimatedWorkerCount = 1;
    public Database database;
    private InstantSource instantSource = InstantSource.system();
    private Directory directory;
    private Duration defaultTtl = Duration.ofMinutes(5);
    private int maxAttempts = 3;
    private Duration defaultThrottle = Duration.ofSeconds(1);
    private TaskSerializer<K> keySerializer;
    private TaskSerializer<T> taskSerializer;

    private Builder(
        Database database,
        Directory directory,
        TaskSerializer<K> keySerializer,
        TaskSerializer<T> taskSerializer) {
      this.database = database;
      this.directory = directory;
      this.keySerializer = keySerializer;
      this.taskSerializer = taskSerializer;
    }

    public Builder<K, T> taskNameExtractor(Function<T, String> taskNameExtractor) {
      this.taskNameExtractor = taskNameExtractor;
      return this;
    }

    /**
     * Sets the directory for the task queue.
     */
    public Builder<K, T> directory(Directory directory) {
      this.directory = directory;
      return this;
    }

    /**
     * Sets the estimated number of workers that will be processing tasks from this queue.
     *
     * @param estimatedWorkerCount the estimated number of workers
     * @return this builder
     */
    public Builder<K, T> estimatedWorkerCount(int estimatedWorkerCount) {
      this.estimatedWorkerCount = estimatedWorkerCount;
      return this;
    }

    /**
     * Sets the instant source for time operations.
     * Default: InstantSource.system()
     */
    public Builder<K, T> instantSource(InstantSource instantSource) {
      this.instantSource = instantSource;
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
