package azkaban.scheduler;

import java.util.function.Consumer;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.SchedulerContext;
import org.quartz.SchedulerException;

public class DelegateJob implements Job {

  private final Consumer delegate;

  /**
   * Create a new DelegatingJob.
   * @param delegate the Runnable implementation to delegate to
   */
  public DelegateJob(final Consumer delegate) {
    this.delegate = delegate;
  }

  /**
   * Return the wrapped Runnable implementation.
   */
  public final Consumer getDelegate() {
    return this.delegate;
  }


  /**
   * Delegates execution to the underlying Runnable.
   */
  @Override
  public void execute(final JobExecutionContext context) throws JobExecutionException {

    SchedulerContext schedulerContext = null;
    try {
      schedulerContext = context.getScheduler().getContext();
    } catch (final SchedulerException e1) {
      e1.printStackTrace();
    }
    final ExternalInstance externalInstance =
        (ExternalInstance) schedulerContext.get("ExternalInstance");

    this.delegate.accept(externalInstance);
  }

}
