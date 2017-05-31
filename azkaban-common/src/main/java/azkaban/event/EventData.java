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
   * @param nestedId node id, corresponds to {@link ExecutableNode#getNestedId()}.
   */
  public EventData(final Status status, final String nestedId) {
    this.status = status;
    this.nestedId = nestedId;
  }

  /**
   * Creates a new EventData instance.
   *
   * @param node node.
   */
  public EventData(final ExecutableNode node) {
    this(node.getStatus(), node.getNestedId());
  }

  public Status getStatus() {
    return this.status;
  }

  public String getNestedId() {
    return this.nestedId;
  }

}
