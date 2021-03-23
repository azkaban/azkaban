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

import static azkaban.Constants.EventReporterConstants.AZ_HOST;
import static azkaban.Constants.EventReporterConstants.AZ_WEBSERVER;
import static azkaban.Constants.EventReporterConstants.EXECUTION_ID;
import static azkaban.Constants.EventReporterConstants.EXECUTOR_TYPE;
import static azkaban.Constants.EventReporterConstants.FLOW_NAME;
import static azkaban.Constants.EventReporterConstants.FLOW_STATUS;
import static azkaban.Constants.EventReporterConstants.FLOW_VERSION;
import static azkaban.Constants.EventReporterConstants.PROJECT_NAME;
import static azkaban.Constants.EventReporterConstants.SUBMIT_TIME;
import static azkaban.Constants.EventReporterConstants.SUBMIT_USER;
import static azkaban.Constants.EventReporterConstants.VERSION_SET;

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

/**
 * This class is the event listener for flow executed in a Kubernetes pod
 * It handles event, gets event meta data, and sends meta data to event reporter plugin
 */
public class PodEventListener implements EventListener<Event> {
  private AzkabanEventReporter azkabanEventReporter;
  private Props props;
  private static final Logger logger = LoggerFactory.getLogger(PodEventListener.class);

  public PodEventListener() {
    this.props = ServiceProvider.SERVICE_PROVIDER.getInstance(Props.class);
    try {
      this.azkabanEventReporter = ServiceProvider.SERVICE_PROVIDER.getInstance(AzkabanEventReporter.class);
    } catch (final Exception e) {
      logger.error("AzkabanEventReporter is not configured");
    }
  }

  @VisibleForTesting
  protected Map<String, String> getFlowMetaData(final ExecutableFlow flow) {
    final Map<String, String> metaData = new HashMap<>();

    // Set up properties not in eventData
    metaData.put(FLOW_NAME, flow.getId());
    metaData.put(AZ_HOST, props.getString(AZ_HOST, "unknown"));
    // As per web server construct, When AZKABAN_WEBSERVER_EXTERNAL_HOSTNAME is set use that,
    // or else use jetty.hostname
    metaData.put(AZ_WEBSERVER, props.getString(AZ_WEBSERVER,
        props.getString("jetty.hostname", "localhost")));
    metaData.put(PROJECT_NAME, flow.getProjectName());
    metaData.put(SUBMIT_USER, flow.getSubmitUser());
    metaData.put(EXECUTION_ID, String.valueOf(flow.getExecutionId()));
    metaData.put(SUBMIT_TIME, String.valueOf(flow.getSubmitTime()));
    metaData.put(FLOW_VERSION, String.valueOf(flow.getAzkabanFlowVersion()));
    metaData.put(FLOW_STATUS, flow.getStatus().name());
    if (flow.getVersionSet() != null) {
      metaData.put(VERSION_SET, getVersionSetJsonString(flow.getVersionSet()));
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
