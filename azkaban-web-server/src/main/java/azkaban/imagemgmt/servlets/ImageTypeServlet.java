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
package azkaban.imagemgmt.servlets;

import azkaban.server.HttpRequestUtils;
import azkaban.server.session.Session;
import azkaban.webapp.AzkabanWebServer;
import azkaban.webapp.servlet.LoginAbstractAzkabanServlet;
import com.linkedin.jersey.api.uri.UriTemplate;
import azkaban.imagemgmt.exeception.ImageMgmtValidationException;
import azkaban.imagemgmt.dto.RequestContext;
import azkaban.imagemgmt.services.ImageTypeService;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.http.HttpStatus;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This servlet exposes the REST APIs such as create, get etc. for image type. Below are the
 * supported APIs.
 * Create Image Type API: POST /imageTypes?session.id=? --data @payload.json
 * Get Image Type API:
 * GET /imageTypes?session.id=?&imageType=?
 * GET /imageTypes/{id}?session.id=?
 */
public class ImageTypeServlet extends LoginAbstractAzkabanServlet {

  private static final String GET_IMAGE_TYPE_URI = "/imageTypes";
  private static final String IMAGE_TYPE_ID_KEY = "id";
  private static final UriTemplate CREATE_IMAGE_TYPE_RESPONSE_URI_TEMPLATE = new UriTemplate(
      String.format("/imageTypes/{%s}", IMAGE_TYPE_ID_KEY));
  private static final UriTemplate GET_IMAGE_TYPE_URI_TEMPLATE = new UriTemplate(
      String.format("/imageTypes/{%s}", IMAGE_TYPE_ID_KEY));
  private ImageTypeService imageTypeService;
  private ObjectMapper objectMapper;

  private static final Logger log = LoggerFactory.getLogger(ImageTypeServlet.class);

  public ImageTypeServlet() {
    super(new ArrayList<>());
  }

  @Override
  public void init(final ServletConfig config) throws ServletException {
    super.init(config);
    final AzkabanWebServer server = (AzkabanWebServer) getApplication();
    this.objectMapper = server.getObjectMapper();
    this.imageTypeService = server.getImageTypeService();
  }

  @Override
  protected void handleGet(HttpServletRequest req, HttpServletResponse resp, Session session)
      throws ServletException, IOException {
    try {
      Map<String, String> templateVariableToValue = new HashMap<>();
      if(GET_IMAGE_TYPE_URI_TEMPLATE.match(req.getRequestURI(), templateVariableToValue)) {
        // TODO: Implementation will be provided in the future PR
      } else if(GET_IMAGE_TYPE_URI.equals(req.getRequestURI())) {
        // TODO: Implementation will be provided in the future PR
      }
    } catch (Exception e) {
      log.error("Content is likely not present " + e);
      resp.setStatus(HttpStatus.SC_NOT_FOUND);
    }
  }

  @Override
  protected void handlePost(HttpServletRequest req, HttpServletResponse resp, Session session)
      throws ServletException, IOException {
    try {
      String jsonPayload = HttpRequestUtils.getBody(req);
      // Build RequestContext DTO to transfer the input request
      RequestContext requestContext = RequestContext.newBuilder()
          .jsonPayload(jsonPayload)
          .user(session.getUser().getUserId())
          .build();
      // Create image type and get image type id
      Integer imageTypeId = imageTypeService.createImageType(requestContext);
      // prepare to send response
      resp.setStatus(HttpStatus.SC_CREATED);
      resp.setHeader("Location", CREATE_IMAGE_TYPE_RESPONSE_URI_TEMPLATE.createURI(imageTypeId.toString()));
      sendResponse(resp, HttpServletResponse.SC_CREATED, new HashMap<>());
    } catch (final ImageMgmtValidationException e) {
      log.error("Input for creating image type is invalid", e);
      sendErrorResponse(resp, HttpServletResponse.SC_BAD_REQUEST,
          "Bad request for creating an image type. Reason: "+e.getMessage());
    } catch (final Exception e) {
      log.error("Exception while creating an image type", e);
      sendErrorResponse(resp, HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
          "Exception while creating an image type. Reason: "+e.getMessage());
    }
  }
}
