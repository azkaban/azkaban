package azkaban.execapp.jmx;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import azkaban.event.Event;
import azkaban.event.EventData;
import azkaban.event.EventListener;
import azkaban.execapp.JobRunner;
import azkaban.executor.ExecutableNode;
import azkaban.executor.Status;
import azkaban.utils.Props;

/**
 * Responsible keeping track of job related MBean attributes through listening
 * to job related events.
 * 
 * @author hluu
 *
 */
public class JmxJobMBeanManager implements JmxJobMXBean, EventListener {

  private static final Logger logger = Logger
      .getLogger(JmxJobMBeanManager.class);

  private static JmxJobMBeanManager INSTANCE = new JmxJobMBeanManager();

  private AtomicInteger runningJobCount = new AtomicInteger(0);
  private AtomicInteger totalExecutedJobCount = new AtomicInteger(0);
  private AtomicInteger totalFailedJobCount = new AtomicInteger(0);
  private AtomicInteger totalSucceededJobCount = new AtomicInteger(0);

  private Map<String, AtomicInteger> jobTypeFailureMap =
      new HashMap<String, AtomicInteger>();

  private Map<String, AtomicInteger> jobTypeSucceededMap =
      new HashMap<String, AtomicInteger>();

  private boolean initialized;

  private JmxJobMBeanManager() {
  }

  public static JmxJobMBeanManager getInstance() {
    return INSTANCE;
  }

  public void initialize(Props props) {
    logger.info("Initializing " + getClass().getName());
    initialized = true;
  }

  @Override
  public int getNumRunningJobs() {
    return runningJobCount.get();
  }

  @Override
  public int getTotalNumExecutedJobs() {
    return totalExecutedJobCount.get();
  }

  @Override
  public int getTotalFailedJobs() {
    return totalFailedJobCount.get();
  }

  @Override
  public int getTotalSucceededJobs() {
    return totalSucceededJobCount.get();
  }

  @Override
  public Map<String, Integer> getTotalSucceededJobsByJobType() {
    return convertMapValueToInteger(jobTypeSucceededMap);
  }

  @Override
  public Map<String, Integer> getTotalFailedJobsByJobType() {
    return convertMapValueToInteger(jobTypeFailureMap);
  }

  private Map<String, Integer> convertMapValueToInteger(
      Map<String, AtomicInteger> map) {
    Map<String, Integer> result = new HashMap<String, Integer>(map.size());

    for (Map.Entry<String, AtomicInteger> entry : map.entrySet()) {
      result.put(entry.getKey(), entry.getValue().intValue());
    }

    return result;
  }

  @Override
  public void handleEvent(Event event) {
    if (!initialized) {
      throw new RuntimeException("JmxJobMBeanManager has not been initialized");
    }

    if (event.getRunner() instanceof JobRunner) {
      JobRunner jobRunner = (JobRunner) event.getRunner();
      EventData eventData = event.getData();
      ExecutableNode node = jobRunner.getNode();

      if (logger.isDebugEnabled()) {
        logger.debug("*** got " + event.getType() + " " + node.getId() + " "
            + event.getRunner().getClass().getName() + " status: "
            + eventData.getStatus());
      }

      if (event.getType() == Event.Type.JOB_STARTED) {
        runningJobCount.incrementAndGet();
      } else if (event.getType() == Event.Type.JOB_FINISHED) {
        totalExecutedJobCount.incrementAndGet();
        if (runningJobCount.intValue() > 0) {
          runningJobCount.decrementAndGet();
        } else {
          logger.warn("runningJobCount not messed up, it is already zero "
              + "and we are trying to decrement on job event "
              + Event.Type.JOB_FINISHED);
        }

        if (eventData.getStatus() == Status.FAILED) {
          totalFailedJobCount.incrementAndGet();
        } else if (eventData.getStatus() == Status.SUCCEEDED) {
          totalSucceededJobCount.incrementAndGet();
        }

        handleJobFinishedCount(eventData.getStatus(), node.getType());
      }

    } else {
      logger.warn("((((((((( Got a different runner: "
          + event.getRunner().getClass().getName());
    }
  }

  private void handleJobFinishedCount(Status status, String jobType) {
    switch (status) {
    case FAILED:
      handleJobFinishedByType(jobTypeFailureMap, jobType);
      break;
    case SUCCEEDED:
      handleJobFinishedByType(jobTypeSucceededMap, jobType);
      break;
    default:
    }
  }

  private void handleJobFinishedByType(Map<String, AtomicInteger> jobTypeMap,
      String jobType) {

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
