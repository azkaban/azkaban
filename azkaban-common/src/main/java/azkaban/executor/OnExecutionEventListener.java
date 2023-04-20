package azkaban.executor;

public interface OnExecutionEventListener {

  /**
   * Perform an execution action when callback method is invoked
   */
  void onExecutionEvent(final ExecutableFlow flow, String action);
}
