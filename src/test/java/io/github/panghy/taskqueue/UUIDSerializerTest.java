package io.github.panghy.taskqueue;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class UUIDSerializerTest {

  private final UUIDSerializer serializer = new UUIDSerializer();

  @Test
  void testSerializeAndDeserialize() {
    UUID uuid = UUID.randomUUID();
    ByteString serialized = serializer.serialize(uuid);
    UUID deserialized = serializer.deserialize(serialized);

    assertThat(deserialized).isEqualTo(uuid);
  }

  @Test
  void testSerializeKnownUUID() {
    UUID uuid = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");
    ByteString serialized = serializer.serialize(uuid);

    assertThat(serialized).isNotNull();
    assertThat(serialized.size()).isEqualTo(16);

    UUID deserialized = serializer.deserialize(serialized);
    assertThat(deserialized).isEqualTo(uuid);
  }

  @Test
  void testRoundTripMultipleUUIDs() {
    for (int i = 0; i < 100; i++) {
      UUID original = UUID.randomUUID();
      ByteString serialized = serializer.serialize(original);
      UUID deserialized = serializer.deserialize(serialized);

      assertThat(deserialized)
          .as("UUID round-trip failed for: " + original)
          .isEqualTo(original);
    }
  }

  @Test
  void testSerializeNilUUID() {
    UUID nilUuid = new UUID(0L, 0L);
    ByteString serialized = serializer.serialize(nilUuid);
    UUID deserialized = serializer.deserialize(serialized);

    assertThat(deserialized).isEqualTo(nilUuid);
  }

  @Test
  void testSerializeMaxUUID() {
    UUID maxUuid = new UUID(-1L, -1L);
    ByteString serialized = serializer.serialize(maxUuid);
    UUID deserialized = serializer.deserialize(serialized);

    assertThat(deserialized).isEqualTo(maxUuid);
  }
}
