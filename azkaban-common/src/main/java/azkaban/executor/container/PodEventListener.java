package azkaban.executor.container;

import static azkaban.Constants.ConfigurationKeys.AZKABAN_SERVER_HOST_NAME;
import static azkaban.Constants.ConfigurationKeys.AZKABAN_WEBSERVER_EXTERNAL_HOSTNAME;

import azkaban.ServiceProvider;
import azkaban.event.Event;
import azkaban.event.EventListener;
import azkaban.executor.ExecutableFlow;
import azkaban.imagemgmt.version.VersionSet;
import azkaban.project.Project;
import azkaban.spi.AzkabanEventReporter;
import azkaban.utils.Props;
import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PodEventListener implements EventListener<Event> {
  private AzkabanEventReporter azkabanEventReporter;
  private static final Logger logger = LoggerFactory.getLogger(PodEventListener.class);

  @VisibleForTesting
  private Map<String, String> getFlowMetaData(final ExecutableFlow flow) {
    final Map<String, String> metaData = new HashMap<>();
    final Props props = ServiceProvider.SERVICE_PROVIDER.getInstance(Props.class);

    // Set up properties not in eventData
    metaData.put("flowName", flow.getId());
    metaData.put("azkabanHost", props.getString(AZKABAN_SERVER_HOST_NAME, "unknown"));
    // As per web server construct, When AZKABAN_WEBSERVER_EXTERNAL_HOSTNAME is set use that,
    // or else use jetty.hostname
    metaData.put("azkabanWebserver", props.getString(AZKABAN_WEBSERVER_EXTERNAL_HOSTNAME,
        props.getString("jetty.hostname", "localhost")));
    metaData.put("projectName", flow.getProjectName());
    metaData.put("submitUser", flow.getSubmitUser());
    metaData.put("executionId", String.valueOf(flow.getExecutionId()));
    metaData.put("submitTime", String.valueOf(flow.getSubmitTime()));
    metaData.put("flowVersion", String.valueOf(flow.getAzkabanFlowVersion()));
    metaData.put("flowStatus", flow.getStatus().name());
    if (flow.getVersionSet() != null && flow.getVersionSet().getImageToVersionMap() != null) {
      metaData.putAll(mapConvert(flow.getVersionSet()));
    }

    return metaData;
  }

  @VisibleForTesting
  private Map<String, String> mapConvert(final VersionSet versionSet) {
    final Map<String, String> imageToVersionMap = new HashMap<>();
    for(String imageType: versionSet.getImageToVersionMap().keySet()) {
      imageToVersionMap.put(imageType,
          versionSet.getImageToVersionMap().get(imageType).getVersion());
    }
    return imageToVersionMap;
  }

  @Override
  public void handleEvent(Event event) {
    try {
      this.azkabanEventReporter = ServiceProvider.SERVICE_PROVIDER.getInstance(AzkabanEventReporter.class);
    } catch (final Exception e) {
      logger.info("AzkabanEventReporter is not configured");
    } finally {
      final ExecutableFlow flow = (ExecutableFlow) event.getRunner();
      this.azkabanEventReporter.report(event.getType(), getFlowMetaData(flow));
    }
  }
}
