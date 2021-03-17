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
 *
 */

package azkaban.webapp.servlet;

import azkaban.server.AzkabanAPI;
import azkaban.webapp.StatusService;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.map.introspect.VisibilityChecker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatusServlet extends AbstractAzkabanServlet {

  private static final Logger logger = LoggerFactory.getLogger(StatusServlet.class);
  private static final long serialVersionUID = -7672556013954947268L;

  private final StatusService statusService;

  public StatusServlet(final StatusService statusService) {
    super(Arrays.asList(new AzkabanAPI("", "")));
    this.statusService = statusService;
  }

  @Override
  protected void doGet(final HttpServletRequest req, final HttpServletResponse resp)
      throws ServletException, IOException {
    try {
      ObjectMapper objectMapper = getObjectMapper();
      String status = objectMapper.writerWithDefaultPrettyPrinter()
          .writeValueAsString(this.statusService.getStatus());
      resp.setContentType(AbstractAzkabanServlet.JSON_MIME_TYPE);
      resp.getOutputStream().println(status);
      resp.setStatus(HttpServletResponse.SC_OK);
    } catch (final Exception e) {
      logger.error("Error!! while reporting status: ", e);
      resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
    } finally {
      resp.getOutputStream().close();
    }
  }

  @VisibleForTesting
  public static ObjectMapper getObjectMapper() {
    ObjectMapper objectMapper = new ObjectMapper();
    VisibilityChecker<?> visibilityChecker = objectMapper.getDeserializationConfig()
        .getDefaultVisibilityChecker()
        .withCreatorVisibility(JsonAutoDetect.Visibility.NONE)
        .withFieldVisibility(JsonAutoDetect.Visibility.NONE)
        .withGetterVisibility(JsonAutoDetect.Visibility.NONE)
        .withIsGetterVisibility(JsonAutoDetect.Visibility.NONE)
        .withSetterVisibility(JsonAutoDetect.Visibility.NONE);
    objectMapper.setVisibilityChecker(visibilityChecker);
    objectMapper.configure(SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS, false);
    return objectMapper;
  }

  @Override
  public Optional<AzkabanAPI> getAzkabanAPI(final HttpServletRequest request) {
    return Optional.of(getApiEndpoints().get(0));
  }
}
