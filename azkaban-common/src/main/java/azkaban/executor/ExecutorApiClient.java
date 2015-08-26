/*
 * Copyright 2015 LinkedIn Corp.
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

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpResponseException;
import org.apache.http.util.EntityUtils;

import azkaban.utils.JSONUtils;
import azkaban.utils.Pair;
import azkaban.utils.RestfulApiClient;

/** Client class that will be used to handle all Restful API calls between Executor and the host application.
 * */
public class ExecutorApiClient extends RestfulApiClient<Map<String, Object>> {

  // hide the constructor. we what this class to be in singleton.
  private ExecutorApiClient(){}

  // international cache for the object instance.
  private static ExecutorApiClient instance = null;

  /**Singleton creator of the class.
   * */
  public static ExecutorApiClient getInstance() {
    if (null == instance) {
      synchronized (ExecutorApiClient.class) {
        if (null == instance) {
          instance = new ExecutorApiClient();
        }
      }
    }
    return instance;
  }

  /**Implementing the parseResponse function to return de-serialized Json object.
   * @param response  the returned response from the HttpClient.
   * */
  @SuppressWarnings("unchecked")
  @Override
  protected Map<String, Object> parseResponse(HttpResponse response)
      throws HttpResponseException, IOException {
    final StatusLine statusLine = response.getStatusLine();
    if (statusLine.getStatusCode() >= 300) {

        logger.error(String.format("unable to parse response as the response status is %s",
            statusLine.getStatusCode()));

        throw new HttpResponseException(statusLine.getStatusCode(),
                statusLine.getReasonPhrase());
    }

    final HttpEntity entity = response.getEntity();
    if (null != entity){
      Object returnVal = JSONUtils.parseJSONFromString(EntityUtils.toString(entity));
      if (null!= returnVal){
        return (Map<String, Object>) returnVal;
      }
    }
    return null;
  }

  /**function to get executor status .
   * @param executorHost    Host name of the executor.
   * @param executorPort    Host port.
   * @param action          query action.
   * @param param           extra query parameters
   * @return  the de-serialized JSON object in Map<String, Object> format.
   * */
  @SuppressWarnings("unchecked")
  public Map<String, Object> callExecutorStats(String executorHost, int executorPort,
      String action, Pair<String, String>... params) throws IOException {

    // form up the URI.
    URI uri = ExecutorApiClient.BuildUri(executorHost, executorPort, "/stats", true,params);
    uri =  ExecutorApiClient.BuildUri(uri, new Pair<String, String>(ConnectorParams.ACTION_PARAM, action));
    return this.httpGet(uri, null);
    }

  // TO-DO  reflector other API call functions out from the ExecutorManager.
}
