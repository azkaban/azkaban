/*
 * Copyright 2017 LinkedIn Corp.
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

import static azkaban.executor.ExecutionControllerUtils.clusterQualifiedExecId;
import static java.util.Objects.requireNonNull;

import azkaban.Constants;
import azkaban.Constants.ConfigurationKeys;
import azkaban.DispatchMethod;
import azkaban.utils.JSONUtils;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import javax.inject.Singleton;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class ExecutorApiGateway {
  private final static Logger logger = LoggerFactory.getLogger(ExecutorApiGateway.class);
  public static final String DEFAULT_CLUSTER_NAME = "azkaban";
  public final static String DEFAULT_EXECUTION_RESOURCE = "executor";
  public final static String CONTAINERIZED_EXECUTION_RESOURCE = "container";

  // Default procedure for modifying a resource path that a reverse proxy, such as an
  // ingress-controller, can use to route the request to correct endpoint.
  //   - This is a first-class function to make it easier to switch to a different mechanism of
  //     creating the path, depending on how the reverse-proxy is configured.
  //   - In future this implementation could be guice-injected (possibly based on a config property)
  //   - This implementation simply prefixes resource name with the execution-id and assumes that
  //     that a reverse proxy can route the request correctly based on this prefix.
  private final static BiFunction<String, String, String> executionResourceNameModifier =
      ((e,r) -> String.join("/",  e, r));

  private final static Executor defaultEmptyExecutor = new Executor(-1, "", 1, false);
  private final ExecutorApiClient apiClient;
  private final String clusterName;

  @Inject
  public ExecutorApiGateway(final ExecutorApiClient apiClient, Props azkProps) {
    requireNonNull(apiClient, "api client must not be null");
    requireNonNull(azkProps, "azkaban properties must not be null");
    this.apiClient = apiClient;
    this.clusterName = azkProps.getString(ConfigurationKeys.AZKABAN_CLUSTER_NAME,
        DEFAULT_CLUSTER_NAME);
  }

  Map<String, Object> callWithExecutable(final ExecutableFlow exflow,
      final Executor executor, final String action) throws ExecutorManagerException {
    return callWithExecutionId(executor.getHost(), executor.getPort(), action,
        exflow.getExecutionId(), null, exflow.getDispatchMethod(), (Pair<String, String>[]) null);
  }

  Map<String, Object> callWithReference(final ExecutionReference ref, final String action,
      final Pair<String, String>... params) throws ExecutorManagerException {
    final Executor executor = (ref.getDispatchMethod() == DispatchMethod.CONTAINERIZED
        ? defaultEmptyExecutor : ref.getExecutor().get());
    return callWithExecutionId(executor.getHost(), executor.getPort(), action, ref.getExecId(),
        null, ref.getDispatchMethod(), params);
  }

  public Map<String, Object> callWithReferenceByUser(final ExecutionReference ref,
      final String action, final String user, final Pair<String, String>... params)
      throws ExecutorManagerException {
    final Executor executor = (ref.getDispatchMethod() == DispatchMethod.CONTAINERIZED
        ? defaultEmptyExecutor : ref.getExecutor().get());
    return callWithExecutionId(executor.getHost(), executor.getPort(), action,
        ref.getExecId(), user, ref.getDispatchMethod(), params);
  }

  @VisibleForTesting
  public String getClusterName() {
    return this.clusterName;
  }

  @VisibleForTesting
  String createExecutionPath(final Optional<Integer> executionId, DispatchMethod dispatchMethod) throws ExecutorManagerException {
    if (null == dispatchMethod || dispatchMethod != DispatchMethod.CONTAINERIZED) {
      return "/" + DEFAULT_EXECUTION_RESOURCE;
    }

    if(!executionId.isPresent()) {
      final String errorMessage = "Execution Id must be provided when reverse-proxy is enabled";
      logger.error(errorMessage);
      throw new ExecutorManagerException(errorMessage);
    }
    return "/" + executionResourceNameModifier.apply(
        clusterQualifiedExecId(clusterName, executionId.get()),
        CONTAINERIZED_EXECUTION_RESOURCE);
  }

  Map<String, Object> callWithExecutionId(final String host, final int port,
      final String action, final Integer executionId, final String user,
      final DispatchMethod dispatchMethod,
      final Pair<String, String>... params) throws ExecutorManagerException {
    try {
      final List<Pair<String, String>> paramList = new ArrayList<>();

      if (params != null) {
        paramList.addAll(Arrays.asList(params));
      }

      paramList
          .add(new Pair<>(ConnectorParams.ACTION_PARAM, action));
      paramList.add(new Pair<>(ConnectorParams.EXECID_PARAM, String
          .valueOf(executionId)));
      paramList.add(new Pair<>(ConnectorParams.USER_PARAM, user));

      // Ideally we should throw an exception if executionId is null but some existing code
      // (updateExecutions()) expects to call this method with a null executionId.
      String executionPath = createExecutionPath(Optional.ofNullable(executionId), dispatchMethod);
      return callForJsonObjectMap(host, port, executionPath, dispatchMethod, paramList);
    } catch (final IOException e) {
      throw new ExecutorManagerException(e.getMessage(), e);
    }
  }

  /**
   * Call executor and parse the JSON response as an instance of the class given as an argument.
   */
  <T> T callForJsonType(final String host, final int port, final String path,
      final DispatchMethod dispatchMethod,
      final List<Pair<String, String>> paramList, final Class<T> valueType) throws IOException {
    final String responseString = callForJsonString(host, port, path, dispatchMethod, paramList);
    if (null == responseString || responseString.length() == 0) {
      return null;
    }
    return new ObjectMapper().readValue(responseString, valueType);
  }

  /*
   * Call executor and return json object map.
   */
  Map<String, Object> callForJsonObjectMap(final String host, final int port,
      final String path, final DispatchMethod dispatchMethod, final List<Pair<String, String>> paramList) throws IOException {
    final String responseString =
        callForJsonString(host, port, path, dispatchMethod, paramList);

    @SuppressWarnings("unchecked") final Map<String, Object> jsonResponse =
        (Map<String, Object>) JSONUtils.parseJSONFromString(responseString);
    final String error = (String) jsonResponse.get(ConnectorParams.RESPONSE_ERROR);
    if (error != null) {
      throw new IOException(error);
    }
    return jsonResponse;
  }

  /*
   * Call executor and return raw json string.
   */
  private String callForJsonString(final String host, final int port, final String path, final DispatchMethod dispatchMethod,
      List<Pair<String, String>> paramList) throws IOException {
    if (paramList == null) {
      paramList = new ArrayList<>();
    }

    @SuppressWarnings("unchecked") final URI uri =
        apiClient.buildExecutorUri(host, port, path, true, dispatchMethod);

    return this.apiClient.doPost(uri, dispatchMethod, paramList);
  }

  public Map<String, Object> updateExecutions(final Executor executor,
      final List<ExecutableFlow> executions) throws ExecutorManagerException {
    final List<Long> updateTimesList = new ArrayList<>();
    final List<Integer> executionIdsList = new ArrayList<>();
    // We pack the parameters of the same host together before query
    for (final ExecutableFlow flow : executions) {
      executionIdsList.add(flow.getExecutionId());
      updateTimesList.add(flow.getUpdateTime());
    }
    final Pair<String, String> updateTimes = new Pair<>(
        ConnectorParams.UPDATE_TIME_LIST_PARAM,
        JSONUtils.toJSON(updateTimesList));
    final Pair<String, String> executionIds = new Pair<>(
        ConnectorParams.EXEC_ID_LIST_PARAM,
        JSONUtils.toJSON(executionIdsList));

    return callWithExecutionId(executor.getHost(), executor.getPort(),
        ConnectorParams.UPDATE_ACTION, null, null, null, executionIds, updateTimes);
  }

}
