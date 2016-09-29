package azkaban.event;

import azkaban.executor.ExecutableNode;
import azkaban.executor.Status;

/**
 * Carries an immutable snapshot of the status data, suitable for asynchronous message passing.
 */
public class EventData {

  private final Status status;
  private final String nestedId;

  /**
   * Creates a new EventData instance.
   *
   * @param status node status.
   */
  public EventData(Status status) {
    this(status, null);
  }

  /**
   * Creates a new EventData instance.
   *
   * @param status node status.
   * @param nestedId node id, corresponds to {@link ExecutableNode#getNestedId()}.
   */
  public EventData(Status status, String nestedId) {
    this.status = status;
    this.nestedId = nestedId;
  }

  public Status getStatus() {
    return status;
  }

  public String getNestedId() {
    return nestedId;
  }

}
