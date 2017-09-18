package azkaban.scheduler;

import org.quartz.Job;
import org.quartz.JobExecutionContext;

public abstract class AbstractQuartzJob implements Job {

  protected static <T> T asT(final Object service) {
    return (T) service;
  }

  @Override
  public abstract void execute(JobExecutionContext context);

  /**
   * Helper method for benefit of subclasses
   */
  protected Object getKey(final JobExecutionContext context, final String key) {
    return context.getMergedJobDataMap().get(key);
  }
}
