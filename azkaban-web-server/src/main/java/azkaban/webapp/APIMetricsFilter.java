/*
 * Copyright 2020 LinkedIn Corp.
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

package azkaban.webapp;

import static azkaban.webapp.servlet.AbstractAzkabanServlet.HTTP_HEADER_AZKABAN_TRACE_ORIGIN;
import static azkaban.webapp.servlet.AbstractAzkabanServlet.HTTP_HEADER_VALUE_AZKABAN_TRACE_ORIGIN;

import azkaban.metrics.AzkabanAPIMetrics;
import azkaban.server.AzkabanAPI;
import azkaban.webapp.servlet.AbstractAzkabanServlet;
import azkaban.webapp.servlet.LoginAbstractAzkabanServlet;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

/**
 * Servlet filter in charge of recording all api endpoint metrics.
 */
public class APIMetricsFilter implements Filter {

  private FilterConfig filterConfig;
  private final Map<String, AbstractAzkabanServlet> routesMap;

  public APIMetricsFilter(final Map<String, AbstractAzkabanServlet> routesMap) {
    this.routesMap = routesMap;
  }

  @Override
  public void doFilter(
      final ServletRequest request,
      final ServletResponse response,
      final FilterChain chain)
      throws IOException, ServletException {
    final HttpServletRequest req = (HttpServletRequest) request;

    final String requestURI = req.getRequestURI();
    final AbstractAzkabanServlet servlet = this.routesMap.get(requestURI);
    if (servlet != null) {
      if (servlet instanceof LoginAbstractAzkabanServlet) {
        final LoginAbstractAzkabanServlet loginServlet = (LoginAbstractAzkabanServlet) servlet;
        if (loginServlet.isUserAuthenticated(req)) {
          // Process authenticated requests
          recordMetrics(chain, req, response, loginServlet);
        } else {
          // Process login requests
          recordOnlyAuthenticationMetrics(chain, req, response, loginServlet);
        }
      } else {
        // Process /status calls
        recordMetrics(chain, req, response, servlet);
      }
    } else {
      // Skip URIs not registered with the filter
      chain.doFilter(request, response);
    }
  }

  @Override
  public void init(final FilterConfig filterConfig) {
    this.filterConfig = filterConfig;
  }

  @Override
  public void destroy() {
    this.filterConfig = null;
  }

  private void recordMetrics(
      final FilterChain chain,
      final HttpServletRequest request,
      final ServletResponse response,
      final AbstractAzkabanServlet servlet) throws IOException, ServletException {
    final long startTime = System.currentTimeMillis();
    final Optional<AzkabanAPI> action = servlet.getAzkabanAPI(request);
    action.ifPresent(a -> recordOriginMetrics(request, a));
    try {
      chain.doFilter(request, response);
    } finally {
      action.ifPresent(a -> a.getMetrics().addResponseTime(System.currentTimeMillis() - startTime));
    }
  }

  private void recordOnlyAuthenticationMetrics(
      final FilterChain chain,
      final HttpServletRequest request,
      final ServletResponse response,
      final LoginAbstractAzkabanServlet servlet) throws IOException, ServletException {
    final long startTime = System.currentTimeMillis();
    final AzkabanAPI loginAPI = servlet.getLoginAPI();
    final String[] actionParam = request.getParameterValues(loginAPI.getRequestParameter());
    if (actionParam != null && Arrays.asList(actionParam).contains(loginAPI.getParameterValue())) {
      recordOriginMetrics(request, loginAPI);
      try {
        chain.doFilter(request, response);
      } finally {
        loginAPI.getMetrics().addResponseTime(System.currentTimeMillis() - startTime);
      }
    } else {
      chain.doFilter(request, response);
    }
  }

  protected void recordOriginMetrics(final HttpServletRequest request, final AzkabanAPI api) {
    final AzkabanAPIMetrics metrics = api.getMetrics();
    final String reqOrigin = request.getHeader(HTTP_HEADER_AZKABAN_TRACE_ORIGIN);
    final boolean isReqFromWebApp = HTTP_HEADER_VALUE_AZKABAN_TRACE_ORIGIN.equals(reqOrigin);
    final String reqHttpMethod = request.getMethod();

    if (isReqFromWebApp && "GET".equals(reqHttpMethod)) {
      metrics.incrementAppGetRequests();
    }
    if (isReqFromWebApp && "POST".equals(reqHttpMethod)) {
      metrics.incrementAppPostRequests();
    }

    if (!isReqFromWebApp && "GET".equals(reqHttpMethod)) {
      metrics.incrementNonAppGetRequests();
    }
    if (!isReqFromWebApp && "POST".equals(reqHttpMethod)) {
      metrics.incrementNonAppPostRequests();
    }
  }
}
