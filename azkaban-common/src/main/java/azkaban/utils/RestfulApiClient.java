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

package azkaban.utils;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.log4j.Logger;

/**
 * class handles the communication between the application and a Restful API based web server.
 *
 * @param T : type of the returning response object. Note: the idea of this abstract class is to
 * provide a wrapper for the logic around HTTP layer communication so development work can take this
 * as a black box and focus on processing the result. With that said the abstract class will be
 * provided as a template, which ideally can support different types of returning object
 * (Dictionary, xmlDoc , text etc.)
 */
public abstract class RestfulApiClient<T> {

  protected static Logger logger = Logger.getLogger(RestfulApiClient.class);

  /**
   * helper function to build a valid URI.
   *
   * @param host host name.
   * @param port host port.
   * @param path extra path after host.
   * @param isHttp indicates if whether Http or HTTPS should be used.
   * @param params extra query parameters.
   * @return the URI built from the inputs.
   */
  public static URI buildUri(final String host, final int port, final String path,
      final boolean isHttp, final Pair<String, String>... params) throws IOException {
    final URIBuilder builder = new URIBuilder();
    builder.setScheme(isHttp ? "http" : "https").setHost(host).setPort(port);

    if (null != path && path.length() > 0) {
      builder.setPath(path);
    }

    if (params != null) {
      for (final Pair<String, String> pair : params) {
        builder.setParameter(pair.getFirst(), pair.getSecond());
      }
    }

    try {
      return builder.build();
    } catch (final URISyntaxException e) {
      throw new IOException(e);
    }
  }

  /**
   * helper function to fill  the request with header entries and posting body .
   */
  private static HttpEntityEnclosingRequestBase completeRequest(
      final HttpEntityEnclosingRequestBase request,
      final List<Pair<String, String>> params) throws UnsupportedEncodingException {
    if (request != null) {
      if (null != params && !params.isEmpty()) {
        final List<NameValuePair> formParams = params.stream()
            .map(pair -> new BasicNameValuePair(pair.getFirst(), pair.getSecond()))
            .collect(Collectors.toList());
        final HttpEntity entity = new UrlEncodedFormEntity(formParams, "UTF-8");
        request.setEntity(entity);
      }
    }
    return request;
  }

  /**
   * Method to transform the response returned by the httpClient into the type specified. Note:
   * Method need to handle case such as failed request. Also method is not supposed to pass the
   * response object out via the returning value as the response will be closed after the execution
   * steps out of the method context.
   **/
  protected abstract T parseResponse(HttpResponse response) throws IOException;

  /**
   * function to perform a Post http request.
   *
   * @param uri    the URI of the request.
   * @param params the form params to be posted, optional.
   * @return the response object type of which is specified by user.
   * @throws UnsupportedEncodingException, IOException
   */
  public T httpPost(final URI uri, final List<Pair<String, String>> params)
      throws IOException {
    // shortcut if the passed url is invalid.
    if (null == uri) {
      logger.error(" unable to perform httpPost as the passed uri is null.");
      return null;
    }

    final HttpPost post = new HttpPost(uri);
    return this.sendAndReturn(completeRequest(post, params));
  }

  /**
   * For returning a HttpClient that will be used for any http requests within this class. This can
   * be overridden by child classes to customize client, for example, for providing a TLS (https)
   * enabled client.
   *
   * @return an http client instance from default settings.
   */
  protected CloseableHttpClient createHttpClient() {
    return HttpClients.createDefault();
  }

  /**
   * function to dispatch the request and pass back the response.
   */
  protected T sendAndReturn(final HttpUriRequest request)
      throws IOException {
    try (final CloseableHttpClient client = this.createHttpClient()) {
      return this.parseResponse(client.execute(request));
    }
  }
}
