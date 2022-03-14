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
import java.net.URI;
import java.util.Collections;
import java.util.List;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpResponseFactory;
import org.apache.http.HttpStatus;
import org.apache.http.HttpVersion;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.DefaultHttpResponseFactory;
import org.apache.http.message.BasicStatusLine;
import org.apache.http.util.EntityUtils;
import org.junit.Assert;
import org.junit.Test;

public class RestfulApiClientTest {

  @Test
  public void testHttpPost() throws Exception {
    final MockRestfulApiClient mockClient = new MockRestfulApiClient();
    final URI uri = MockRestfulApiClient.buildUri("test.com", 80, "test", true,
        new Pair<>("Entry1", "Value1"));

    final String content = "123456789";

    final String result = mockClient.httpPost(uri, -1, toPairList(content));
    Assert.assertTrue(result != null && result.contains(uri.toString()));
    Assert.assertTrue(result.contains("METHOD = POST"));
    Assert.assertTrue(result.contains(String.format("%s = value=%s;", "BODY", content)));
  }

  private List<Pair<String, String>> toPairList(final String content) {
    return Collections.singletonList(new Pair<>("value", content));
  }

  static class MockRestfulApiClient extends RestfulApiClient<String> {

    private final int status = HttpStatus.SC_OK;

    @Override
    protected String parseResponse(final HttpResponse response) throws IOException {
      final StatusLine statusLine = response.getStatusLine();
      if (statusLine.getStatusCode() >= 300) {
        throw new HttpResponseException(statusLine.getStatusCode(),
            statusLine.getReasonPhrase());
      }
      return EntityUtils.toString(response.getEntity());
    }

    @Override
    protected String sendAndReturn(final HttpUriRequest request, final int HttpTimeout) throws IOException {
      final HttpResponseFactory factory = new DefaultHttpResponseFactory();

      final HttpResponse response = factory.newHttpResponse(
          new BasicStatusLine(HttpVersion.HTTP_1_1, this.status, null), null);

      final StringBuilder sb = new StringBuilder();
      sb.append(String.format("%s = %s;", "METHOD", request.getMethod()));
      sb.append(String.format("%s = %s;", "URI", request.getURI()));

      final HttpEntity entity = ((HttpEntityEnclosingRequestBase) request).getEntity();
      sb.append(String.format("%s = %s;", "BODY", EntityUtils.toString(entity)));

      response.setEntity(new StringEntity(sb.toString()));
      return parseResponse(response);
    }

  }
}
