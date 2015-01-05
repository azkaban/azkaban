package azkaban.metric;

/**
 * Dummy Metric to test Azkaban Metrics
 */
public class FakeMetric extends AbstractMetric<Integer>{

  public FakeMetric(MetricReportManager manager) {
    super("FakeMetric", "int", 4, manager);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((metricManager == null) ? 0 : metricManager.hashCode());
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    result = prime * result + ((type == null) ? 0 : type.hashCode());
    result = prime * result + ((value == null) ? 0 : value.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (!(obj instanceof FakeMetric))
      return false;
    FakeMetric other = (FakeMetric) obj;
    if (metricManager == null) {
      if (other.metricManager != null)
        return false;
    } else if (!metricManager.equals(other.metricManager))
      return false;
    if (name == null) {
      if (other.name != null)
        return false;
    } else if (!name.equals(other.name))
      return false;
    if (type == null) {
      if (other.type != null)
        return false;
    } else if (!type.equals(other.type))
      return false;
    if (value == null) {
      if (other.value != null)
        return false;
    } else if (!value.equals(other.value))
      return false;
    return true;
  }
}
