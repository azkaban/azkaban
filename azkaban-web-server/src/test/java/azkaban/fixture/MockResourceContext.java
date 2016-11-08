/*
 * Copyright 2016 LinkedIn Corp.
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

package azkaban.fixture;

import com.linkedin.data.transform.filter.request.MaskTree;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.restli.server.PathKeys;
import com.linkedin.restli.server.ProjectionMode;
import com.linkedin.restli.server.ResourceContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class MockResourceContext implements ResourceContext {

    Map<String, String> requestHeaders;
    RequestContext requestContext;

    @Override
    public RestRequest getRawRequest() {
        return null;
    }

    @Override
    public String getRequestMethod() {
        return null;
    }

    @Override
    public PathKeys getPathKeys() {
        return null;
    }

    @Override
    public MaskTree getProjectionMask() {
        return null;
    }

    @Override
    public boolean hasParameter(String key) {
        return false;
    }

    @Override
    public String getParameter(String key) {
        return null;
    }

    @Override
    public Object getStructuredParameter(String key) {
        return null;
    }

    @Override
    public List<String> getParameterValues(String key) {
        return null;
    }

    public void setRequestHeader(String name, String value){

        this.requestHeaders.put(name, value);
    }

    @Override
    public Map<String, String> getRequestHeaders() {
        return this.requestHeaders;
    }

    @Override
    public void setResponseHeader(String name, String value) {

    }

    @Override
    public RequestContext getRawRequestContext() {
        return this.requestContext;
    }

    @Override
    public ProjectionMode getProjectionMode() {
        return null;
    }

    @Override
    public void setProjectionMode(ProjectionMode mode) {

    }

    public void setLocalAttr(String name, String value){
        requestContext.putLocalAttr(name, value);
    }

    public MockResourceContext(){
        requestHeaders = new HashMap<>();
        requestContext = new RequestContext();
    }

    public static MockResourceContext getResourceContextWithUpstream(String clientIp, String upstream){
        MockResourceContext ctx = new MockResourceContext();

        ctx.setLocalAttr("REMOTE_ADDR", clientIp);
        ctx.setRequestHeader("X-Forwarded-For", upstream);

        return ctx;
    }

    public static MockResourceContext getResourceContextWithMultipleUpstreams(String clientIp,
                                                                              String firstUpstream){
        MockResourceContext ctx = new MockResourceContext();

        ctx.setLocalAttr("REMOTE_ADDR", clientIp);
        ctx.setRequestHeader("X-Forwarded-For", firstUpstream + ",55.55.55.55:55555,1.1.1.1:9999");

        return ctx;
    }

    public static MockResourceContext getResourceContext(String clientIp){
        MockResourceContext ctx = new MockResourceContext();

        ctx.setLocalAttr("REMOTE_ADDR", clientIp);

        return ctx;
    }

}
