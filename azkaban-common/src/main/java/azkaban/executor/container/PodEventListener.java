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

import static azkaban.Constants.EventReporterConstants;

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
 * It handles event, gets event metadata, and sends metadata to event reporter plugin
 */
public class PodEventListener implements EventListener<Event> {
  private AzkabanEventReporter azkabanEventReporter;
  private Props props;
  private static final Logger logger = LoggerFactory.getLogger(PodEventListener.class);

  public PodEventListener() {
    try {
      this.props = ServiceProvider.SERVICE_PROVIDER.getInstance(Props.class);
      this.azkabanEventReporter = ServiceProvider.SERVICE_PROVIDER.getInstance(AzkabanEventReporter.class);
    } catch (final Exception e) {
      logger.error("AzkabanEventReporter is not configured");
    }
  }

  /**
   * Extracts flow metadata from an executable flow, and save the data to a map
   * @param flow
   * @return flow metadata
   */
  @VisibleForTesting
  protected Map<String, String> getFlowMetaData(final ExecutableFlow flow) {
    final Map<String, String> metaData = new HashMap<>();

    // Set up properties not in eventData
    metaData.put(EventReporterConstants.FLOW_NAME, flow.getId());
    if (props != null) {
      metaData.put(EventReporterConstants.AZ_HOST, props.getString(EventReporterConstants.AZ_HOST
          , "unknown"));
      // As per web server construct, When AZKABAN_WEBSERVER_EXTERNAL_HOSTNAME is set use that,
      // or else use jetty.hostname
      metaData.put(EventReporterConstants.AZ_WEBSERVER,
          props.getString(EventReporterConstants.AZ_WEBSERVER,
          props.getString("jetty.hostname", "localhost")));
    }
    metaData.put(EventReporterConstants.PROJECT_NAME, flow.getProjectName());
    metaData.put(EventReporterConstants.SUBMIT_USER, flow.getSubmitUser());
    metaData.put(EventReporterConstants.EXECUTION_ID, String.valueOf(flow.getExecutionId()));
    metaData.put(EventReporterConstants.SUBMIT_TIME, String.valueOf(flow.getSubmitTime()));
    metaData.put(EventReporterConstants.FLOW_VERSION, String.valueOf(flow.getAzkabanFlowVersion()));
    metaData.put(EventReporterConstants.FLOW_STATUS, flow.getStatus().name());
    if (flow.getVersionSet() != null) { // Flow version set is set when flow is
      // executed in a container, which also indicates executor type is Kubernetes.
      metaData.put(EventReporterConstants.VERSION_SET,
          getVersionSetJsonString(flow.getVersionSet()));
      metaData.put(EventReporterConstants.EXECUTOR_TYPE, String.valueOf(ExecutorType.KUBERNETES));
    } else {
      metaData.put(EventReporterConstants.EXECUTOR_TYPE, String.valueOf(ExecutorType.BAREMETAL));
    }

    return metaData;
  }

  /**
   * Converts a VersionSet object into a version set json string, which are key value
   * pairs of image name and its corresponding version number
   * @param versionSet
   * @return a version set json string
   */
  private String getVersionSetJsonString (final VersionSet versionSet){
    final Map<String, String> imageToVersionStringMap = new HashMap<>();
    for (final String imageType: versionSet.getImageToVersionMap().keySet()){
      imageToVersionStringMap.put(imageType,
          versionSet.getImageToVersionMap().get(imageType).getVersion());
    }

    return JSONUtils.toJSON(imageToVersionStringMap, true);
  }

  /**
   * Implements method handleEvent in event reporter and sends flow life cycle event of a
   * containerized execution
   * @param event
   */
  @Override
  public void handleEvent(final Event event) {
    if (this.azkabanEventReporter != null) {
      final ExecutableFlow flow = (ExecutableFlow) event.getRunner();
      if (flow != null) {
        this.azkabanEventReporter.report(event.getType(), getFlowMetaData(flow));
      }
    }
  }
}
