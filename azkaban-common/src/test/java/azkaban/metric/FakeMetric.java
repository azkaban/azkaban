package azkaban.metric;

/**
 * Dummy Metric to test Azkaban Metrics
 */
public class FakeMetric extends AbstractMetric<Integer> {

  public FakeMetric(final MetricReportManager manager) {
    super("FakeMetric", "int", 4, manager);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((this.metricManager == null) ? 0 : this.metricManager.hashCode());
    result = prime * result + ((this.name == null) ? 0 : this.name.hashCode());
    result = prime * result + ((this.type == null) ? 0 : this.type.hashCode());
    result = prime * result + ((this.value == null) ? 0 : this.value.hashCode());
    return result;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof FakeMetric)) {
      return false;
    }
    final FakeMetric other = (FakeMetric) obj;
    if (this.metricManager == null) {
      if (other.metricManager != null) {
        return false;
      }
    } else if (!this.metricManager.equals(other.metricManager)) {
      return false;
    }
    if (this.name == null) {
      if (other.name != null) {
        return false;
      }
    } else if (!this.name.equals(other.name)) {
      return false;
    }
    if (this.type == null) {
      if (other.type != null) {
        return false;
      }
    } else if (!this.type.equals(other.type)) {
      return false;
    }
    if (this.value == null) {
      if (other.value != null) {
        return false;
      }
    } else if (!this.value.equals(other.value)) {
      return false;
    }
    return true;
  }
}
