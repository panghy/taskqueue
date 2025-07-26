package io.github.panghy.taskqueue;

import static org.junit.Assert.*;

import com.apple.foundationdb.tuple.Tuple;
import java.time.Duration;
import org.junit.Test;

public class TaskQueueConfigTest {

  @Test
  public void testBuilderDefaults() {
    TaskQueueConfig<String, String> config = TaskQueueConfig.<String, String>builder()
        .keyPrefix(Tuple.from("test"))
        .keySerializer(new StringSerializer())
        .taskSerializer(new StringSerializer())
        .build();

    assertEquals(Duration.ofMinutes(5), config.getDefaultTtl());
    assertEquals(3, config.getMaxAttempts());
    assertEquals(Duration.ofSeconds(1), config.getDefaultThrottle());
  }

  @Test
  public void testBuilderCustomValues() {
    TaskQueueConfig<String, String> config = TaskQueueConfig.<String, String>builder()
        .keyPrefix(Tuple.from("test"))
        .defaultTtl(Duration.ofMinutes(10))
        .maxAttempts(5)
        .defaultThrottle(Duration.ofSeconds(5))
        .keySerializer(new StringSerializer())
        .taskSerializer(new StringSerializer())
        .build();

    assertEquals(Duration.ofMinutes(10), config.getDefaultTtl());
    assertEquals(5, config.getMaxAttempts());
    assertEquals(Duration.ofSeconds(5), config.getDefaultThrottle());
  }

  @Test(expected = NullPointerException.class)
  public void testBuilderRequiresKeyPrefix() {
    TaskQueueConfig.<String, String>builder()
        .keySerializer(new StringSerializer())
        .taskSerializer(new StringSerializer())
        .build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidMaxAttempts() {
    TaskQueueConfig.<String, String>builder()
        .keyPrefix(Tuple.from("test"))
        .maxAttempts(0)
        .keySerializer(new StringSerializer())
        .taskSerializer(new StringSerializer())
        .build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidTtlZero() {
    TaskQueueConfig.<String, String>builder()
        .keyPrefix(Tuple.from("test"))
        .defaultTtl(Duration.ZERO)
        .keySerializer(new StringSerializer())
        .taskSerializer(new StringSerializer())
        .build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidTtlNegative() {
    TaskQueueConfig.<String, String>builder()
        .keyPrefix(Tuple.from("test"))
        .defaultTtl(Duration.ofMinutes(-1))
        .keySerializer(new StringSerializer())
        .taskSerializer(new StringSerializer())
        .build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidThrottleNegative() {
    TaskQueueConfig.<String, String>builder()
        .keyPrefix(Tuple.from("test"))
        .defaultThrottle(Duration.ofSeconds(-1))
        .keySerializer(new StringSerializer())
        .taskSerializer(new StringSerializer())
        .build();
  }

  @Test(expected = NullPointerException.class)
  public void testBuilderRequiresKeySerializer() {
    TaskQueueConfig.<String, String>builder()
        .keyPrefix(Tuple.from("test"))
        .taskSerializer(new StringSerializer())
        .build();
  }

  @Test(expected = NullPointerException.class)
  public void testBuilderRequiresTaskSerializer() {
    TaskQueueConfig.<String, String>builder()
        .keyPrefix(Tuple.from("test"))
        .keySerializer(new StringSerializer())
        .build();
  }

  private static class StringSerializer implements TaskQueueConfig.TaskSerializer<String> {
    @Override
    public byte[] serialize(String value) {
      return value != null ? value.getBytes() : new byte[0];
    }

    @Override
    public String deserialize(byte[] bytes) {
      return bytes != null && bytes.length > 0 ? new String(bytes) : null;
    }
  }
}
