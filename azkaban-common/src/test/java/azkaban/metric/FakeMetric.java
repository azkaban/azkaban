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
    result = prime * result + ((_metricManager == null) ? 0 : _metricManager.hashCode());
    result = prime * result + ((_name == null) ? 0 : _name.hashCode());
    result = prime * result + ((_type == null) ? 0 : _type.hashCode());
    result = prime * result + ((_value == null) ? 0 : _value.hashCode());
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
    if (_metricManager == null) {
      if (other._metricManager != null)
        return false;
    } else if (!_metricManager.equals(other._metricManager))
      return false;
    if (_name == null) {
      if (other._name != null)
        return false;
    } else if (!_name.equals(other._name))
      return false;
    if (_type == null) {
      if (other._type != null)
        return false;
    } else if (!_type.equals(other._type))
      return false;
    if (_value == null) {
      if (other._value != null)
        return false;
    } else if (!_value.equals(other._value))
      return false;
    return true;
  }
}
