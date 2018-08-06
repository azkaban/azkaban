/*
 * Copyright 2018 LinkedIn Corp.
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

package azkaban.jobtype.tuning;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.httpclient.HttpStatus;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.mockito.Mock;

import azkaban.flow.CommonJobProperties;
import azkaban.jobtype.HadoopConfigurationInjector;
import azkaban.security.commons.HadoopSecurityManager;
import azkaban.utils.Props;

import com.google.gson.Gson;


/**
 * TuningParameterUtils is a utility class responsible for updating tuned hadoop parameters for the job
 * by calling tuning api. Before calling tuning api, it gets default parameters for the job and passes default
 * parameters to api. By default it tries 3 times in case api is down before running the job with default parameters.
 */
public class TuningParameterUtils {

  private Logger log = Logger.getRootLogger();

  private CloseableHttpClient httpClient;

  private final int DEFAULT_TUNING_API_RETRY_COUNT = 3;
  private final int DEFAULT_TUNING_API_TIMEOUT_MS = 10000;

  private final String TUNING_API_RETRY_COUNT = "tuning.api.retry.count";
  private final String TUNING_API_TIMEOUT = "tuning.api.timeout";
  private final String AZKABAN = "azkaban";
  private final String TUNING_API_VERSION = "2";

  public TuningParameterUtils(CloseableHttpClient httpClient) {
    this.httpClient = httpClient;
  }

  public TuningParameterUtils() {
    httpClient = HttpClients.createDefault();
  }

  /**
   * Get hadoop properties from the job
   * @param props
   * @return Map of parameter name and value
   */
  public Map<String, String> getHadoopProperties(Props props) {
    Map<String, String> confProperties = props.getMapByPrefix(HadoopConfigurationInjector.INJECT_PREFIX);
    if (log.isDebugEnabled()) {
      for (Map.Entry<String, String> entry : confProperties.entrySet()) {
        log.debug("Adding default parameter " + entry.getKey() + ", value " + entry.getValue());
      }
    }
    return confProperties;
  }

  /**
   * This method calls Auto Tuning Framework Rest API and adds new parameters in props.
   * This method will retry 3 times in case there is timeout while calling rest API.
   * @param props
   */
  public void updateAutoTuningParameters(Props props) {
    int retryCount = 0;
    Map<String, String> params = null;
    if (!props.containsKey(TuningCommonConstants.TUNING_API_END_POINT)) {
      log.error("Tuning API End Point not found in properties. Not applying auto tuning. ");
      return;
    }
    int maxRetryCount = DEFAULT_TUNING_API_RETRY_COUNT;
    if (props.containsKey(TUNING_API_RETRY_COUNT)) {
      maxRetryCount = props.getInt(TUNING_API_RETRY_COUNT);
    }

    while (retryCount < maxRetryCount) {
      try {
        retryCount++;
        log.info("Calling get current run parameters. Try count " + retryCount);
        params = getCurrentRunParameters(props);
        break;
      } catch (Exception e) {
        log.error("Error in getting current run parameter ", e);
      }
    }
    log.info("Out of while loop " + retryCount);
    //In case parameters are null, do nothing and job will run with default parameters of the job
    if (params != null) {
      for (Map.Entry<String, String> param : params.entrySet()) {
        props.put(HadoopConfigurationInjector.INJECT_PREFIX + param.getKey(), param.getValue());
        log.info("Applied parameter " + param.getKey() + " with value " + param.getValue());
      }
      props.put(HadoopConfigurationInjector.INJECT_PREFIX + TuningCommonConstants.AUTO_TUNING_ENABLED, "true");
    }
    log.info("Out of if clause " + retryCount);
  }

  /**
   * This method is for getting current run parameters from Dr Elephant
   * @param props Azkaban Props object which contains information like Job Definition, Job Execution etc
   * @return Map of parameter name and value
   * @throws IOException
   * @throws ParseException
   */
  @SuppressWarnings({ "unchecked" })
  public Map<String, String> getCurrentRunParameters(Props props) throws IOException {

    log.debug("Properties are " + props.toString());

    HttpResponse response = null;
    String endPoint = props.get(TuningCommonConstants.TUNING_API_END_POINT);

    log.info("Dr elephant endPoint : " + endPoint);

    int tuningAPITimeoutMS = DEFAULT_TUNING_API_TIMEOUT_MS;
    if (props.containsKey(TUNING_API_TIMEOUT)) {
      tuningAPITimeoutMS = props.getInt(TUNING_API_TIMEOUT);
    }

    log.info("Dr elephant endPoint timeout " + tuningAPITimeoutMS);

    HttpPost request = new HttpPost(endPoint);
    final RequestConfig config =
        RequestConfig.custom().setSocketTimeout(tuningAPITimeoutMS).setConnectTimeout(tuningAPITimeoutMS)
            .setConnectionRequestTimeout(tuningAPITimeoutMS).build();
    request.setConfig(config);

    Map<String, String> params = new HashMap<String, String>();

    params.put(TuningCommonConstants.PROJECT_NAME_API_PARAM, props.get(CommonJobProperties.PROJECT_NAME));

    params.put(TuningCommonConstants.FLOW_DEFINITION_ID_API_PARAM, props.get(CommonJobProperties.WORKFLOW_LINK));

    params.put(TuningCommonConstants.JOB_DEFINITION_ID_API_PARAM, props.get(CommonJobProperties.JOB_LINK));

    params.put(TuningCommonConstants.FLOW_EXECUTION_ID_API_PARAM, props.get(CommonJobProperties.EXECUTION_LINK));

    params.put(TuningCommonConstants.JOB_EXECUTION_ID_API_PARAM, props.get(CommonJobProperties.ATTEMPT_LINK));

    params.put(TuningCommonConstants.FLOW_DEFINITION_URL_API_PARAM, props.get(CommonJobProperties.WORKFLOW_LINK));

    params.put(TuningCommonConstants.JOB_DEFINITION_URL_API_PARAM, props.get(CommonJobProperties.JOB_LINK));

    params.put(TuningCommonConstants.FLOW_EXECUTION_URL_API_PARAM, props.get(CommonJobProperties.EXECUTION_LINK));

    params.put(TuningCommonConstants.JOB_EXECUTION_URL_API_PARAM, props.get(CommonJobProperties.ATTEMPT_LINK));

    params.put(TuningCommonConstants.JOB_NAME_API_PARAM, props.get(CommonJobProperties.JOB_ID));

    params.put(TuningCommonConstants.DEFAULT_PARAM_API_PARAM, getDefaultJobParameterJSON(props));

    params.put(TuningCommonConstants.SCHEDULER_API_PARAM, AZKABAN);

    params.put(TuningCommonConstants.CLIENT_API_PARAM, AZKABAN);

    params.put(TuningCommonConstants.AUTO_TUNING_JOB_TYPE_API_PARAM,
        props.get(TuningCommonConstants.AUTO_TUNING_JOB_TYPE));

    params.put(TuningCommonConstants.OPTIMIZATION_METRIC_API_PARAM,
        props.get(TuningCommonConstants.OPTIMIZATION_METRIC));

    params.put(TuningCommonConstants.USER_NAME_API_PARAM, props.get(HadoopSecurityManager.USER_TO_PROXY));

    if (props.containsKey(TuningCommonConstants.AUTO_TUNING_RETRY)) {
      params.put(TuningCommonConstants.IS_RETRY_API_PARAM, props.get(TuningCommonConstants.AUTO_TUNING_RETRY));
    } else {
      params.put(TuningCommonConstants.IS_RETRY_API_PARAM, "false");
    }

    params.put(TuningCommonConstants.SKIP_EXECUTION_FOR_OPTIMIZATION_API_PARAM, "false");

    if (props.containsKey(TuningCommonConstants.ALLOWED_MAX_EXECUTION_TIME_PERCENT)) {
      params.put(TuningCommonConstants.ALLOWED_MAX_EXECUTION_TIME_PERCENT_API_PARAM,
          props.getString(TuningCommonConstants.ALLOWED_MAX_EXECUTION_TIME_PERCENT));
    }

    if (props.containsKey(TuningCommonConstants.ALLOWED_MAX_RESOURCE_USAGE_PERCENT)) {
      params.put(TuningCommonConstants.ALLOWED_MAX_RESOURCE_USAGE_PERCENT_PERCENT_API_PARAM,
          props.getString(TuningCommonConstants.ALLOWED_MAX_RESOURCE_USAGE_PERCENT));
    }
    params.put(TuningCommonConstants.VERSION_API_PARAM, TUNING_API_VERSION);

    Gson gson = new Gson();
    StringEntity stringEntity = new StringEntity(gson.toJson(params));
    request.setEntity(stringEntity);

    request.addHeader("content-type", "application/json");
    log.info("Request starting ");
    response = httpClient.execute(request);
    log.info("Request complete ");

    Map<String, String> outputParams = null;
    // check for the response code
    if (response != null) {
      if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
        log.error("Error in response. Response is " + response);
        request.releaseConnection();
        throw new IOException("Error in response. ");
      } else {
        HttpEntity responseEntity = response.getEntity();
        String responseJson = EntityUtils.toString(responseEntity);
        outputParams = (Map<String, String>) gson.fromJson(responseJson, Map.class);
        request.releaseConnection();
      }
    }
    log.info("Response parsed.");

    httpClient.close();
    return outputParams;
  }

  /**
   * Get json for default job parameters values
   * @param props Azkaban Props object
   * @return JSON string of default hadoop parameter map
   */
  public String getDefaultJobParameterJSON(Props props) {
    Map<String, String> paramValueMap = getHadoopProperties(props);
    Gson gson = new Gson();
    String jobParamsJson = gson.toJson(paramValueMap);
    return jobParamsJson;
  }
}
