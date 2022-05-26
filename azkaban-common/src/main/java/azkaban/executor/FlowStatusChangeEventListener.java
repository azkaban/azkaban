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
package azkaban.executor;

import static azkaban.Constants.ConfigurationKeys.JETTY_HOSTNAME;
import static azkaban.Constants.EventReporterConstants;

import azkaban.DispatchMethod;
import azkaban.ServiceProvider;
import azkaban.event.Event;
import azkaban.event.EventListener;
import azkaban.imagemgmt.version.VersionSet;
import azkaban.spi.AzkabanEventReporter;
import azkaban.spi.ExecutorType;
import azkaban.utils.JSONUtils;
import azkaban.utils.Props;
import com.google.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;

/**
 * This class reports FLOW_STATUS_CHANGED events in web server, flow statuses include
 * e.g. READY, DISPATCHING, PREPARING
 * It handles event, prepares event metadata, and sends metadata to event reporter plugin
 */
public class FlowStatusChangeEventListener implements EventListener<Event> {
  private AzkabanEventReporter azkabanEventReporter;
  private Props props;
  private static final Logger logger = Logger.getLogger(FlowStatusChangeEventListener.class);

  @Inject
  public FlowStatusChangeEventListener(final Props props) {
    try {
      this.props = props;
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
  public synchronized Map<String, String> getFlowMetaData(final ExecutableFlow flow) {
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
          props.getString(JETTY_HOSTNAME, "localhost")));
    }
    metaData.put(EventReporterConstants.PROJECT_NAME, flow.getProjectName());
    metaData.put(EventReporterConstants.SUBMIT_USER, flow.getSubmitUser());
    metaData.put(EventReporterConstants.EXECUTION_ID, String.valueOf(flow.getExecutionId()));
    metaData.put(EventReporterConstants.SUBMIT_TIME, String.valueOf(flow.getSubmitTime()));
    metaData.put(EventReporterConstants.FLOW_VERSION, String.valueOf(flow.getAzkabanFlowVersion()));
    metaData.put(EventReporterConstants.FLOW_STATUS, flow.getStatus().name());
    metaData.put(EventReporterConstants.EXECUTION_RETRIED_BY_AZKABAN,
        String.valueOf(flow.getExecutionOptions().isExecutionRetried()));
    if (flow.getExecutionOptions().getOriginalFlowExecutionIdBeforeRetry() != null) {
      // original flow execution id is set when there is one
      metaData.put(EventReporterConstants.ORIGINAL_FLOW_EXECUTION_ID_BEFORE_RETRY,
          String.valueOf(flow.getExecutionOptions().getOriginalFlowExecutionIdBeforeRetry()));
    }
    if (flow.getVersionSet() != null) { // Save version set information
      metaData.put(EventReporterConstants.VERSION_SET,
          getVersionSetJsonString(flow.getVersionSet()));
    }
    if (flow.getDispatchMethod() == DispatchMethod.CONTAINERIZED) { // Determine executor type
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
  public synchronized void handleEvent(final Event event) {
    if (this.azkabanEventReporter != null && event.getRunner() instanceof ExecutableFlow) {
      final ExecutableFlow flow = (ExecutableFlow) event.getRunner();
      if (flow != null) {
        this.azkabanEventReporter.report(event.getType(), getFlowMetaData(flow));
      }
    }
  }
}
