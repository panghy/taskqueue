package io.github.panghy.taskqueue;

import static com.google.protobuf.ByteString.EMPTY;
import static com.google.protobuf.ByteString.copyFromUtf8;

/**
 * Serializer for String values.
 */
class StringSerializer implements TaskQueueConfig.TaskSerializer<String> {
  @Override
  public com.google.protobuf.ByteString serialize(String value) {
    return value != null ? copyFromUtf8(value) : EMPTY;
  }

  @Override
  public String deserialize(com.google.protobuf.ByteString bytes) {
    return bytes != null && !bytes.isEmpty() ? bytes.toStringUtf8() : null;
  }
}
