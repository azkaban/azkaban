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
import java.util.ArrayList;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpResponseFactory;
import org.apache.http.HttpStatus;
import org.apache.http.HttpVersion;
import org.apache.http.NameValuePair;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.DefaultHttpResponseFactory;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.message.BasicStatusLine;
import org.apache.http.util.EntityUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class RestfulApiClientTest {

  protected class MockRestfulApiClient extends RestfulApiClient<String> {
    private int  status = HttpStatus.SC_OK;

    @Override
    protected String parseResponse(HttpResponse response) throws IOException {
      final StatusLine statusLine = response.getStatusLine();
      if (statusLine.getStatusCode() >= 300) {
          throw new HttpResponseException(statusLine.getStatusCode(),
                  statusLine.getReasonPhrase());
      }
      final HttpEntity entity = response.getEntity();
      return entity == null ? null : EntityUtils.toString(entity);
    }

    public void setReturnStatus(int newStatus){
      this.status = newStatus;
    }

    public void resetReturnStatus(){
      this.status = HttpStatus.SC_OK;
    }

    @Override
    protected String sendAndReturn(HttpUriRequest request) throws IOException{
      HttpResponseFactory factory = new DefaultHttpResponseFactory();

      HttpResponse response = factory.newHttpResponse(
          new BasicStatusLine(HttpVersion.HTTP_1_1, this.status, null),null);

      StringBuilder sb = new StringBuilder();
      sb.append(String.format("%s = %s;", "METHOD", request.getMethod()));
      sb.append(String.format("%s = %s;", "URI", request.getURI()));

      if (request.getAllHeaders().length > 0){
        sb.append("HEADER_EXISTS");
      }

      for (Header h : request.getAllHeaders()){
        sb.append(String.format("%s = %s;", h.getName(), h.getValue()));
      }

      if (request instanceof HttpEntityEnclosingRequestBase){
        HttpEntity entity = ((HttpEntityEnclosingRequestBase)request).getEntity();
        if (entity != null){
          sb.append("BODY_EXISTS");
          sb.append(String.format("%s = %s;", "BODY", EntityUtils.toString(entity)));
        }
      }

      response.setEntity(new StringEntity(sb.toString()));
      return parseResponse(response);
    }

  }

  @Test
  public void testHttpGet() throws Exception {
    MockRestfulApiClient mockClient = new MockRestfulApiClient();
    @SuppressWarnings("unchecked")
    URI uri = MockRestfulApiClient.buildUri("test.com", 80, "test", true,
        new Pair <String,String>("Entry1","Value1"));

    String result = mockClient.httpGet(uri, null);
    Assert.assertTrue(result!= null && result.contains(uri.toString()));
    Assert.assertTrue(result.contains("METHOD = GET"));
  }

  @Test
  public void testHttpGetWithHeaderItems() throws Exception {
    MockRestfulApiClient mockClient = new MockRestfulApiClient();
    @SuppressWarnings("unchecked")
    URI uri = MockRestfulApiClient.buildUri("test.com", 80, "test", true,
        new Pair <String,String>("Entry1","Value1"));

    ArrayList<NameValuePair> headerItems = new ArrayList<NameValuePair>();
    headerItems.add(new BasicNameValuePair("h1","v1"));
    headerItems.add(new BasicNameValuePair("h2","v2"));

    String result = mockClient.httpGet(uri, headerItems);
    Assert.assertTrue(result!= null && result.contains(uri.toString()));
    Assert.assertTrue(result.contains("METHOD = GET"));
    Assert.assertTrue(result.contains("h1 = v1"));
    Assert.assertTrue(result.contains("h2 = v2"));
  }

  @Test
  public void testHttpPost() throws Exception {
    MockRestfulApiClient mockClient = new MockRestfulApiClient();
    @SuppressWarnings("unchecked")
    URI uri = MockRestfulApiClient.buildUri("test.com", 80, "test", true,
        new Pair <String,String>("Entry1","Value1"));

    ArrayList<NameValuePair> headerItems = new ArrayList<NameValuePair>();
    headerItems.add(new BasicNameValuePair("h1","v1"));
    headerItems.add(new BasicNameValuePair("h2","v2"));

    String content = "123456789";

    String result = mockClient.httpPost(uri, headerItems,content);
    Assert.assertTrue(result!= null && result.contains(uri.toString()));
    Assert.assertTrue(result.contains("METHOD = POST"));
    Assert.assertTrue(result.contains("h1 = v1"));
    Assert.assertTrue(result.contains("h2 = v2"));
    Assert.assertTrue(result.contains(String.format("%s = %s;", "BODY", content)));
  }

  @Test
  public void testHttpPostWOBody() throws Exception {
    MockRestfulApiClient mockClient = new MockRestfulApiClient();
    @SuppressWarnings("unchecked")
    URI uri = MockRestfulApiClient.buildUri("test.com", 80, "test", true,
        new Pair <String,String>("Entry1","Value1"));

    String result = mockClient.httpPost(uri, null,null);
    Assert.assertTrue(result!= null && result.contains(uri.toString()));
    Assert.assertTrue(result.contains("METHOD = POST"));
    Assert.assertFalse(result.contains("BODY_EXISTS"));
    Assert.assertFalse(result.contains("HEADER_EXISTS"));
  }

  @Test
  public void testHttpPut() throws Exception {
    MockRestfulApiClient mockClient = new MockRestfulApiClient();
    @SuppressWarnings("unchecked")
    URI uri = MockRestfulApiClient.buildUri("test.com", 80, "test", true,
        new Pair <String,String>("Entry1","Value1"));

    ArrayList<NameValuePair> headerItems = new ArrayList<NameValuePair>();
    headerItems.add(new BasicNameValuePair("h1","v1"));
    headerItems.add(new BasicNameValuePair("h2","v2"));

    String content = "123456789";

    String result = mockClient.httpPut(uri, headerItems,content);
    Assert.assertTrue(result!= null && result.contains(uri.toString()));
    Assert.assertTrue(result.contains("METHOD = PUT"));
    Assert.assertTrue(result.contains("h1 = v1"));
    Assert.assertTrue(result.contains("h2 = v2"));
    Assert.assertTrue(result.contains(String.format("%s = %s;", "BODY", content)));
  }

  @Test
  public void testContentLength() throws Exception {
    MockRestfulApiClient mockClient = new MockRestfulApiClient();
    @SuppressWarnings("unchecked")
    URI uri = MockRestfulApiClient.buildUri("test.com", 80, "test", true,
        new Pair <String,String>("Entry1","Value1"));

    String content = "123456789";

    String result = mockClient.httpPut(uri, null,content);
    Assert.assertTrue(result!= null && result.contains(uri.toString()));
    Assert.assertTrue(result.contains("Content-Length = " + Integer.toString(content.length())));
  }

  @Test
  public void testContentLengthOverride() throws Exception {
    MockRestfulApiClient mockClient = new MockRestfulApiClient();
    @SuppressWarnings("unchecked")
    URI uri = MockRestfulApiClient.buildUri("test.com", 80, "test", true,
        new Pair <String,String>("Entry1","Value1"));

    ArrayList<NameValuePair> headerItems = new ArrayList<NameValuePair>();
    headerItems.add(new BasicNameValuePair("Content-Length","0"));

    String content = "123456789";

    String result = mockClient.httpPut(uri, headerItems,content);
    Assert.assertTrue(result!= null && result.contains(uri.toString()));
    Assert.assertEquals(result.lastIndexOf("Content-Length"),result.indexOf("Content-Length"));
    Assert.assertTrue(result.contains("Content-Length = " + Integer.toString(content.length())));
  }

  @Test
  public void testHttpDelete() throws Exception {
    MockRestfulApiClient mockClient = new MockRestfulApiClient();
    @SuppressWarnings("unchecked")
    URI uri = MockRestfulApiClient.buildUri("test.com", 80, "test", true,
        new Pair <String,String>("Entry1","Value1"));

    ArrayList<NameValuePair> headerItems = new ArrayList<NameValuePair>();
    headerItems.add(new BasicNameValuePair("h1","v1"));
    headerItems.add(new BasicNameValuePair("h2","v2"));

    String result = mockClient.httpDelete(uri, headerItems);
    Assert.assertTrue(result!= null && result.contains(uri.toString()));
    Assert.assertTrue(result.contains("METHOD = DELETE"));
    Assert.assertTrue(result.contains("h1 = v1"));
    Assert.assertTrue(result.contains("h2 = v2"));
  }
}
