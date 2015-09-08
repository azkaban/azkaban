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
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpResponseException;
import org.apache.http.util.EntityUtils;
import org.codehaus.jackson.map.ObjectMapper;

import azkaban.utils.RestfulApiClient;

/** Client class that will be used to handle all Restful API calls between Executor and the host application.
 * */
public class ExecutorApiClient<T extends java.io.Serializable> extends RestfulApiClient<T> {
  private final Class<T> typeOfclass;

  /**
   * Constructor of the class.
   * @param typeOfclass the type of class that the T represents. Must provide.
   * */
  public ExecutorApiClient(Class<T> typeOfclass){
    if (null == typeOfclass){
      throw new IllegalArgumentException("Class type of the returning object must be specified.");
    }
    this.typeOfclass = typeOfclass;
  }

  /**Implementing the parseResponse function to return de-serialized Json object.
   * @param response  the returned response from the HttpClient.
   * @return de-serialized object from Json or null if the response doesn't have a body.
   * */
  @Override
  protected T parseResponse(HttpResponse response)
      throws HttpResponseException, IOException {
    final StatusLine statusLine = response.getStatusLine();
    String responseBody = response.getEntity() != null ?
        EntityUtils.toString(response.getEntity()) : "";

    if (statusLine.getStatusCode() >= 300) {

        logger.error(String.format("unable to parse response as the response status is %s",
            statusLine.getStatusCode()));

        throw new HttpResponseException(statusLine.getStatusCode(),responseBody);
    }

    final HttpEntity entity = response.getEntity();
    if (null == entity || entity.getContentLength() >= Integer.MAX_VALUE){
      logger.error("unable to parse the response as the response is null or with an invlaid length");
      return null;
    }
    return new ObjectMapper().readValue(EntityUtils.toString(entity), this.typeOfclass);
  }
}
