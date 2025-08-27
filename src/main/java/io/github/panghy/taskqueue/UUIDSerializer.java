package io.github.panghy.taskqueue;

import com.google.protobuf.ByteString;
import java.util.UUID;

public class UUIDSerializer implements TaskQueueConfig.TaskSerializer<UUID> {

  @Override
  public ByteString serialize(UUID value) {
    byte[] bytes = KeyedTaskQueue.uuidToBytes(value);
    return ByteString.copyFrom(bytes);
  }

  @Override
  public UUID deserialize(ByteString bytes) {
    return KeyedTaskQueue.bytesToUuid(bytes.toByteArray());
  }
}
