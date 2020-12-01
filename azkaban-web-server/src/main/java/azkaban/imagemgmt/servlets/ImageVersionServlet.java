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

import static azkaban.Constants.ImageMgmtConstants.ID_KEY;

import azkaban.Constants.ImageMgmtConstants;
import azkaban.imagemgmt.dto.ImageMetadataRequest;
import azkaban.imagemgmt.exeception.ImageMgmtValidationException;
import azkaban.imagemgmt.models.ImageVersion.State;
import azkaban.imagemgmt.services.ImageVersionService;
import azkaban.server.HttpRequestUtils;
import azkaban.server.session.Session;
import azkaban.webapp.AzkabanWebServer;
import azkaban.webapp.servlet.LoginAbstractAzkabanServlet;
import com.google.common.primitives.Ints;
import com.linkedin.jersey.api.uri.UriTemplate;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
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
 * supported APIs. Create Image Version API: POST /imageVersions?session.id=? --data @payload.json
 * Search/Get Image Versions API: GET /imageVersions?session.id=?&imageType=?&imageVersion=?&versionState=?
 * GET /imageVersions/{id}?session.id=? Update image version API: POST
 * /imageVersions/{id}?session.id=? --data @payload.json
 */
public class ImageVersionServlet extends LoginAbstractAzkabanServlet {

  private static final String BASE_IMAGE_VERSION_URI = "/imageVersions";
  private static final String IMAGE_VERSION_ID_KEY = "id";
  private static final UriTemplate SINGLE_IMAGE_VERSION_URI_TEMPLATE = new UriTemplate(
      String.format("/imageVersions/{%s}", IMAGE_VERSION_ID_KEY));
  private ImageVersionService imageVersionService;
  private ObjectMapper objectMapper;

  private static final Logger log = LoggerFactory.getLogger(ImageVersionServlet.class);

  public ImageVersionServlet() {
    super(new ArrayList<>());
  }

  @Override
  public void init(final ServletConfig config) throws ServletException {
    super.init(config);
    final AzkabanWebServer server = (AzkabanWebServer) getApplication();
    this.objectMapper = server.getObjectMapper();
    this.imageVersionService = server.getImageVersionsService();
  }

  @Override
  protected void handleGet(HttpServletRequest req, HttpServletResponse resp, Session session)
      throws ServletException, IOException {
    /* Get specific record */
    try {
      String response = null;
      if (BASE_IMAGE_VERSION_URI.equals(req.getRequestURI())) {
        // imageType must present. If not present throws ServletException
        String imageType = HttpRequestUtils.getParam(req, ImageMgmtConstants.IMAGE_TYPE);
        // imageVersion is optional. Hence can be null
        Optional<String> imageVersion = Optional.ofNullable(HttpRequestUtils.getParam(req,
            ImageMgmtConstants.IMAGE_VERSION,
            null));
        // imageVersion is optional. Hence can be null
        String versionStateString = HttpRequestUtils.getParam(req, ImageMgmtConstants.VERSION_STATE,
            null);
        Optional<State> versionState =
            Optional.ofNullable(versionStateString != null ? State.valueOf(versionStateString) :
                null);
        // create RequestContext DTO to transfer the input request
        ImageMetadataRequest imageMetadataRequest = ImageMetadataRequest.newBuilder()
            .addParam(ImageMgmtConstants.IMAGE_TYPE, imageType) // mandatory parameter
            .addParamIfPresent(ImageMgmtConstants.IMAGE_VERSION, imageVersion) // optional parameter
            .addParamIfPresent(ImageMgmtConstants.VERSION_STATE, versionState) // optional parameter
            .build();
        // invoke service method and get response in string format
        response =
            objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(
                imageVersionService.findImageVersions(imageMetadataRequest));
      }
      this.writeResponse(resp, response);
    } catch (final Exception e) {
      log.error("Requested image metadata not found " + e);
      sendErrorResponse(resp, HttpServletResponse.SC_NOT_FOUND,
          "Requested image metadata not found. Reason : " + e.getMessage());
    }
  }

  @Override
  protected void handlePost(HttpServletRequest req, HttpServletResponse resp, Session session)
      throws ServletException, IOException {
    final Map<String, String> templateVariableToValue = new HashMap<>();
    log.info("URI Template: " + SINGLE_IMAGE_VERSION_URI_TEMPLATE.toString());
    log.info("req uri: " + req.getRequestURI());
    if (BASE_IMAGE_VERSION_URI.equals(req.getRequestURI())) {
      log.info("Inside update image version");
      this.handleCreateImageVersion(req, resp, session);
    } else if (SINGLE_IMAGE_VERSION_URI_TEMPLATE.match(req.getRequestURI(),
        templateVariableToValue)) {
      log.info("Inside update image version");
      this.handleUpdateImageVersion(req, resp, session, templateVariableToValue);
    } else {
      // Unsupported route, return an error.
      log.error("Invalid route for imageVersions endpoint: " + req.getRequestURI());
      sendErrorResponse(resp, HttpServletResponse.SC_NOT_FOUND,
          "The requested resource was not found.");
    }
  }

  private void handleCreateImageVersion(final HttpServletRequest req,
      final HttpServletResponse resp, final Session session) throws ServletException, IOException {
    try {
      String jsonPayload = HttpRequestUtils.getBody(req);
      // Build ImageMetadataRequest DTO to transfer the input request
      ImageMetadataRequest imageMetadataRequest = ImageMetadataRequest.newBuilder()
          .jsonPayload(jsonPayload)
          .user(session.getUser().getUserId())
          .build();
      // Create image version metadata and image version id
      Integer imageVersionId = imageVersionService.createImageVersion(imageMetadataRequest);
      // prepare to send response
      resp.setStatus(HttpStatus.SC_CREATED);
      resp.setHeader("Location",
          SINGLE_IMAGE_VERSION_URI_TEMPLATE.createURI(imageVersionId.toString()));
      sendResponse(resp, HttpServletResponse.SC_CREATED, new HashMap<>());
    } catch (final ImageMgmtValidationException e) {
      log.error("Input for creating image version metadata is invalid", e);
      sendErrorResponse(resp, HttpServletResponse.SC_BAD_REQUEST,
          "Bad request for creating image version metadata");
    } catch (final Exception e) {
      log.error("Exception while creating image version metadata", e);
      sendErrorResponse(resp, HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
          "Exception while creating image version metadata. " + e.getMessage());
    }
  }

  private void handleUpdateImageVersion(final HttpServletRequest req,
      final HttpServletResponse resp, final Session session,
      final Map<String, String> templateVariableToValue) throws ServletException,
      IOException {
    try {
      String idString = templateVariableToValue.get(IMAGE_VERSION_ID_KEY);
      log.info("idString: " + idString);
      final Integer id = Ints.tryParse(idString);
      if (id == null) {
        log.error("Invalid image version id: ", idString);
        throw new ImageMgmtValidationException("Image version id is invalid");
      }
      String jsonPayload = HttpRequestUtils.getBody(req);
      // Build ImageMetadataRequest DTO to transfer the input request
      ImageMetadataRequest imageMetadataRequest = ImageMetadataRequest.newBuilder()
          .jsonPayload(jsonPayload)
          .user(session.getUser().getUserId())
          .addParam(ID_KEY, id)
          .build();
      // Create image version metadata and image version id
      imageVersionService.updateImageVersion(imageMetadataRequest);
      sendResponse(resp, HttpServletResponse.SC_OK, new HashMap<>());
    } catch (final ImageMgmtValidationException e) {
      log.error("Input for updating image version metadata is invalid", e);
      sendErrorResponse(resp, HttpServletResponse.SC_BAD_REQUEST,
          "Bad request for updating image version metadata");
    } catch (final Exception e) {
      log.error("Exception while updating image version metadata", e);
      sendErrorResponse(resp, HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
          "Exception while updating image version metadata. " + e.getMessage());
    }
  }

}
