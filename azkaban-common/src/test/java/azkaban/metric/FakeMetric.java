package azkaban.metric;

/**
 * Dummy Metric to test Azkaban Metrics
 */
public class FakeMetric extends AbstractMetric<Integer>{

  public FakeMetric(MetricReportManager manager) {
    super("FakeMetric", "int", 4, manager);
  }

  @Override
  public boolean equals(Object obj) {
      if (obj == this) {
          return true;
      }
      if (obj == null || obj.getClass() != this.getClass()) {
          return false;
      }
      FakeMetric metric = (FakeMetric) obj;
      return metric.getName() == getName() && metric.getValue() == getValue();
  }
}
