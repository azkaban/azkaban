package azkaban.scheduler;

import java.io.Serializable;
import java.util.Map;

public class QuartzJobDescription {

  private final String groupName;
  private final Class<? extends AbstractQuartzJob> jobClass;
  private final Map<String, ? extends Serializable> contextMap;

  public QuartzJobDescription(final Class<? extends AbstractQuartzJob> jobClass,
      final String groupName,
      final Map<String, ? extends Serializable> contextMap) {
    this.jobClass = jobClass;
    this.groupName = groupName;
    this.contextMap = contextMap;
  }

  public Class<? extends AbstractQuartzJob> getJobClass() {
    return this.jobClass;
  }

  public Map<String, ? extends Serializable> getContextMap() {
    return this.contextMap;
  }

  @Override
  public String toString() {
    return "QuartzJobDescription{" +
        "jobClass=" + this.jobClass +
        ", groupName='" + this.groupName + '\'' +
        ", contextMap=" + this.contextMap +
        '}';
  }

  public String getGroupName() {
    return this.groupName;
  }
}
