package io.github.panghy.taskqueue;

public class TaskQueueException extends RuntimeException {
  public TaskQueueException(String message) {
    super(message);
  }

  public TaskQueueException(String message, Throwable cause) {
    super(message, cause);
  }
}
