package azkaban.execapp.jmx;

import azkaban.event.Event;
import azkaban.event.EventData;
import azkaban.event.EventListener;
import azkaban.execapp.YARNJobRunner;
import azkaban.executor.ExecutableNode;
import azkaban.executor.Status;
import azkaban.spi.EventType;
import azkaban.utils.Props;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.log4j.Logger;

/**
 * Responsible keeping track of job related MBean attributes through listening to job related
 * events.
 *
 * @author hluu
 */
public class YARNJmxJobMBeanManager implements JmxJobMXBean, EventListener {

  private static final Logger logger = Logger
      .getLogger(YARNJmxJobMBeanManager.class);

  private static final YARNJmxJobMBeanManager INSTANCE = new YARNJmxJobMBeanManager();

  private final AtomicInteger runningJobCount = new AtomicInteger(0);
  private final AtomicInteger totalExecutedJobCount = new AtomicInteger(0);
  private final AtomicInteger totalFailedJobCount = new AtomicInteger(0);
  private final AtomicInteger totalSucceededJobCount = new AtomicInteger(0);

  private final Map<String, AtomicInteger> jobTypeFailureMap =
      new HashMap<>();

  private final Map<String, AtomicInteger> jobTypeSucceededMap =
      new HashMap<>();

  private boolean initialized;

  private YARNJmxJobMBeanManager() {
  }

  public static YARNJmxJobMBeanManager getInstance() {
    return INSTANCE;
  }

  public void initialize(final Props props) {
    logger.info("Initializing " + getClass().getName());
    this.initialized = true;
  }

  @Override
  public int getNumRunningJobs() {
    return this.runningJobCount.get();
  }

  @Override
  public int getTotalNumExecutedJobs() {
    return this.totalExecutedJobCount.get();
  }

  @Override
  public int getTotalFailedJobs() {
    return this.totalFailedJobCount.get();
  }

  @Override
  public int getTotalSucceededJobs() {
    return this.totalSucceededJobCount.get();
  }

  @Override
  public Map<String, Integer> getTotalSucceededJobsByJobType() {
    return convertMapValueToInteger(this.jobTypeSucceededMap);
  }

  @Override
  public Map<String, Integer> getTotalFailedJobsByJobType() {
    return convertMapValueToInteger(this.jobTypeFailureMap);
  }

  private Map<String, Integer> convertMapValueToInteger(
      final Map<String, AtomicInteger> map) {
    final Map<String, Integer> result = new HashMap<>(map.size());

    for (final Map.Entry<String, AtomicInteger> entry : map.entrySet()) {
      result.put(entry.getKey(), entry.getValue().intValue());
    }

    return result;
  }

  @Override
  public void handleEvent(final Event event) {
    if (!this.initialized) {
      throw new RuntimeException("YARNJmxJobMBeanManager has not been initialized");
    }

    if (event.getRunner() instanceof YARNJobRunner) {
      final YARNJobRunner jobRunner = (YARNJobRunner) event.getRunner();
      final EventData eventData = event.getData();
      final ExecutableNode node = jobRunner.getNode();

      if (logger.isDebugEnabled()) {
        logger.debug("*** got " + event.getType() + " " + node.getId() + " "
            + event.getRunner().getClass().getName() + " status: "
            + eventData.getStatus());
      }

      if (event.getType() == EventType.JOB_STARTED) {
        this.runningJobCount.incrementAndGet();
      } else if (event.getType() == EventType.JOB_FINISHED) {
        this.totalExecutedJobCount.incrementAndGet();
        if (this.runningJobCount.intValue() > 0) {
          this.runningJobCount.decrementAndGet();
        } else {
          logger.warn("runningJobCount not messed up, it is already zero "
              + "and we are trying to decrement on job event "
              + EventType.JOB_FINISHED);
        }

        if (eventData.getStatus() == Status.FAILED) {
          this.totalFailedJobCount.incrementAndGet();
        } else if (eventData.getStatus() == Status.SUCCEEDED) {
          this.totalSucceededJobCount.incrementAndGet();
        }

        handleJobFinishedCount(eventData.getStatus(), node.getType());
      }

    } else {
      logger.warn("((((((((( Got a different runner: "
          + event.getRunner().getClass().getName());
    }
  }

  private void handleJobFinishedCount(final Status status, final String jobType) {
    switch (status) {
      case FAILED:
        handleJobFinishedByType(this.jobTypeFailureMap, jobType);
        break;
      case SUCCEEDED:
        handleJobFinishedByType(this.jobTypeSucceededMap, jobType);
        break;
      default:
    }
  }

  private void handleJobFinishedByType(final Map<String, AtomicInteger> jobTypeMap,
      final String jobType) {

    synchronized (jobTypeMap) {
      AtomicInteger count = jobTypeMap.get(jobType);
      if (count == null) {
        count = new AtomicInteger();
      }

      count.incrementAndGet();
      jobTypeMap.put(jobType, count);
    }
  }
}
