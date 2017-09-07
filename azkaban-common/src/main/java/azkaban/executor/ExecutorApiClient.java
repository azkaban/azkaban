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

import azkaban.utils.RestfulApiClient;
import java.io.IOException;
import javax.inject.Singleton;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpResponseException;
import org.apache.http.util.EntityUtils;

/**
 * Client class that will be used to handle all Restful API calls between Executor and the host
 * application.
 */
@Singleton
public class ExecutorApiClient extends RestfulApiClient<String> {

  /**
   * Implementing the parseResponse function to return de-serialized Json object.
   *
   * @param response the returned response from the HttpClient.
   * @return de-serialized object from Json or null if the response doesn't have a body.
   */
  @Override
  protected String parseResponse(final HttpResponse response)
      throws HttpResponseException, IOException {
    final StatusLine statusLine = response.getStatusLine();
    final String responseBody = response.getEntity() != null ?
        EntityUtils.toString(response.getEntity()) : "";

    if (statusLine.getStatusCode() >= 300) {

      logger.error(String.format("unable to parse response as the response status is %s",
          statusLine.getStatusCode()));

      throw new HttpResponseException(statusLine.getStatusCode(), responseBody);
    }

    return responseBody;
  }
}
