package azkaban.scheduler;

import com.google.inject.Injector;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.quartz.Job;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.spi.JobFactory;
import org.quartz.spi.TriggerFiredBundle;

@Singleton
public class SchedulerJobFactory implements JobFactory {

  @Inject
  private Injector injector;

  @Inject
  public SchedulerJobFactory(final Injector injector) {
    this.injector = injector;
  }

  @Override
  public Job newJob(final TriggerFiredBundle bundle, final Scheduler scheduler)
      throws SchedulerException {
    return (Job) this.injector.getInstance(bundle.getJobDetail()
        .getJobClass());
  }
}
