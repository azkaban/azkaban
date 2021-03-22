/*
 * Copyright 2021 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package azkaban.executor.container;

import static azkaban.Constants.ConfigurationKeys.AZKABAN_SERVER_HOST_NAME;
import static azkaban.Constants.ConfigurationKeys.AZKABAN_WEBSERVER_EXTERNAL_HOSTNAME;
import static azkaban.Constants.EXECUTOR_TYPE;

import azkaban.ServiceProvider;
import azkaban.event.Event;
import azkaban.event.EventListener;
import azkaban.executor.ExecutableFlow;
import azkaban.imagemgmt.version.VersionSet;
import azkaban.spi.AzkabanEventReporter;
import azkaban.spi.ExecutorType;
import azkaban.utils.JSONUtils;
import azkaban.utils.Props;
import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PodEventListener implements EventListener<Event> {
  private AzkabanEventReporter azkabanEventReporter;
  private static final Logger logger = LoggerFactory.getLogger(PodEventListener.class);

  public PodEventListener() {
    try {
      this.azkabanEventReporter = ServiceProvider.SERVICE_PROVIDER.getInstance(AzkabanEventReporter.class);
    } catch (final Exception e) {
      logger.info("AzkabanEventReporter is not configured");
    }
  }

  @VisibleForTesting
  synchronized Map<String, String> getFlowMetaData(final ExecutableFlow flow) {
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
    if (flow.getVersionSet() != null) {
      metaData.put("versionSet", getVersionSetJsonString(flow.getVersionSet()));
      metaData.put(EXECUTOR_TYPE, String.valueOf(ExecutorType.KUBERNETES));
    } else {
      metaData.put(EXECUTOR_TYPE, String.valueOf(ExecutorType.BAREMETAL));
    }

    return metaData;
  }

  private String getVersionSetJsonString (final VersionSet versionSet){
    final Map<String, String> imageToVersionStringMap = new HashMap<>();
    for (final String imageType: versionSet.getImageToVersionMap().keySet()){
      imageToVersionStringMap.put(imageType,
          versionSet.getImageToVersionMap().get(imageType).getVersion());
    }

    return JSONUtils.toJSON(imageToVersionStringMap, true);
  }

  @Override
  public void handleEvent(final Event event) {
    if (this.azkabanEventReporter != null) {
      final ExecutableFlow flow = (ExecutableFlow) event.getRunner();
      this.azkabanEventReporter.report(event.getType(), getFlowMetaData(flow));
    }
  }
}
