package azkaban.webapp;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URL;
import java.net.URLEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AmfSender {
  private static final Logger LOGGER = LoggerFactory.getLogger(AmfSender.class);

  private String _hostName;
  private String _appName;
  private String _serverUrl;
  private URL _url;

  public AmfSender(String appName, String serverUrl)
      throws Exception {
    this._appName = appName;
    this._serverUrl = serverUrl;
    _hostName = InetAddress.getLocalHost().getCanonicalHostName();
    _url = new URL(serverUrl);
    LOGGER.info("AmfSender appName {}, serverUrl: {}", appName, serverUrl);
  }

  private void sendData(String data)
      throws Exception {
    HttpURLConnection connection;
    connection = (HttpURLConnection) _url.openConnection();
    connection.setRequestMethod("POST");
    connection.setDoOutput(true);
    PrintWriter networkWriter;
    networkWriter = new PrintWriter(connection.getOutputStream());
    networkWriter.write(data);
    networkWriter.close();
    int result = connection.getResponseCode();
    BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));

    String line;
    while ((line = in.readLine()) != null) {
      LOGGER.debug("Response: " + line);
    }
  }

  public void putMetrics(Iterable<MetricEntry> metrics)
      throws Exception {
    LOGGER.debug("putMetrics called");

    StringBuilder lines = new StringBuilder();

    lines.append("host=");
    lines.append(_hostName);
    lines.append("&");
    lines.append("appname=");
    lines.append(_appName);
    lines.append("&");
    lines.append("metrics=");
    StringBuilder metricsLines = new StringBuilder();
    metricsLines.append("[");
    // Collect data points.
    boolean isFirst = true;
    for (MetricEntry metric : metrics) {
      if (!isFirst) {
        metricsLines.append(",");
      }

      String type;
      if (metric.isCounter()) {
        type = MetricType.COUNTER.name();
      } else {
        type = MetricType.GAUGE.name();
      }

      metricsLines.append("{\"metric\":\"")
          .append(metric.getName().replace(' ', '.'))
          .append("\",\"time\":")
          .append(metric.getTime())
          .append(",\"value\":")
          .append(metric.getValue())
          .append(",\"metric_type\":\"")
          .append(type)
          .append("\"}");
      isFirst = false;
    }
    metricsLines.append("]");
    String metricsLinesString = metricsLines.toString();
    String encoded = "";
    encoded = URLEncoder.encode(metricsLinesString, "UTF-8");
    lines.append(encoded);
    String data = lines.toString();
    sendData(data);
  }
}

enum MetricType {
  COUNTER, GAUGE
}
