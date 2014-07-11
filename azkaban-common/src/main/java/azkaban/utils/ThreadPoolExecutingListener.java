package azkaban.utils;

/**
 * Interface for listener to get notified before and after a task has been
 * executed.
 * 
 * @author hluu
 * 
 */
public interface ThreadPoolExecutingListener {
  public void beforeExecute(Runnable r);

  public void afterExecute(Runnable r);
}
