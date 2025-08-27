package io.github.panghy.taskqueue;

import io.github.panghy.taskqueue.proto.Task;
import io.github.panghy.taskqueue.proto.TaskKey;
import io.github.panghy.taskqueue.proto.TaskKeyMetadata;
import lombok.Builder;

@Builder
public record TaskClaim<K, T>(
    Task taskProto,
    TaskKey taskKeyProto,
    TaskKeyMetadata taskKeyMetadataProto,
    TaskQueue<K, T> taskQueue,
    K taskKey,
    T task) {

  public void complete() {
    taskQueue.completeTask(this);
  }

  public void fail() {
    taskQueue.failTask(this);
  }
}
