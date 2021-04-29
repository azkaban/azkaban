package azkaban.executor.container;

/**
 * Defines methods for lifecycle management of a resource watch corresponding to a
 * {@link ContainerizedImpl}
 */
public interface ContainerizedWatch {

  /**
   * Initialize the watch and start processing any received events.
   * Note that implementations have the flexibility of the either starting the watch processing
   * in a separate thread or alternatively this can block the calling thread and use it for the
   * processing.
   */
  public void launchWatch();

  /**
   * Submit a shutdown request for the watch instance.
   * @return 'true' if the shutdown was not already requested, 'false' otherwise.
   */
  public boolean requestShutdown();
}
