package azkaban.scheduler;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class QuartzSampleJob implements Job {

  @Override
  public void execute(final JobExecutionContext context)
      throws JobExecutionException {
    System.out.println("Hello Quartz!  " + context.getTrigger().toString() + context
        .getScheduledFireTime());
  }
}
