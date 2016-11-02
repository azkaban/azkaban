package azkaban.soloserver;

import com.codahale.metrics.*;
import java.util.concurrent.TimeUnit;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;

public class TryMetrics {
  static final MetricRegistry metrics = new MetricRegistry();

  public static void main(String args[]) {
    Meter requests = metrics.meter("requests");
    metrics.register("jvm/memory", new MemoryUsageGaugeSet());
    requests.mark();
    wait5Seconds();
    startReport();
  }

  static void startReport() {

    String serverUrl = "http://lva1-amf.corp.linkedin.com/api/v1/metrics";
//    String serverUrl = "http://127.0.0.1:5000/";

    try {
      final AMFReporter amfReporter = new AMFReporter(metrics, serverUrl);
      amfReporter.start(5, TimeUnit.SECONDS);

//      ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
//          .convertRatesTo(TimeUnit.SECONDS)
//          .convertDurationsTo(TimeUnit.MILLISECONDS)
//          .build();
//      reporter.start(1, TimeUnit.SECONDS);
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  static void wait5Seconds() {
    try {
      Thread.sleep(5*1000);
    } catch (InterruptedException e) {}
  }
}