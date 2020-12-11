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
import static azkaban.Constants.ImageMgmtConstants.IMAGE_TYPE;

import azkaban.Constants.ImageMgmtConstants;
import azkaban.imagemgmt.dto.ImageMetadataRequest;
import azkaban.imagemgmt.exeception.ImageMgmtInvalidPermissionException;
import azkaban.imagemgmt.exeception.ImageMgmtValidationException;
import azkaban.imagemgmt.models.ImageVersion.State;
import azkaban.imagemgmt.services.ImageVersionService;
import azkaban.server.HttpRequestUtils;
import azkaban.server.session.Session;
import azkaban.user.Permission.Type;
import azkaban.utils.JSONUtils;
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
  protected void handleGet(
      final HttpServletRequest req, final HttpServletResponse resp, final Session session)
      throws ServletException, IOException {
    /* Get specific record */
    try {
      String response = null;
      if (BASE_IMAGE_VERSION_URI.equals(req.getRequestURI())) {
        // imageType must present. If not present throws ServletException
        final String imageType = HttpRequestUtils.getParam(req, ImageMgmtConstants.IMAGE_TYPE);
        // Check for required permission to invoke the API
        if (!hasImageManagementPermission(imageType, session.getUser(), Type.GET)) {
          log.debug(String.format("Invalid permission to get image version "
              + "for user: %s image type: %s.", session.getUser().getUserId(), imageType));
          throw new ImageMgmtInvalidPermissionException("Invalid permission to get image version");
        }
        // imageVersion is optional. Hence can be null
        final Optional<String> imageVersion = Optional.ofNullable(HttpRequestUtils.getParam(req,
            ImageMgmtConstants.IMAGE_VERSION,
            null));
        // imageVersion is optional. Hence can be null
        final String versionStateString = HttpRequestUtils.getParam(req, ImageMgmtConstants.VERSION_STATE,
            null);
        final Optional<State> versionState =
            Optional.ofNullable(versionStateString != null ? State.valueOf(versionStateString) :
                null);
        // create RequestContext DTO to transfer the input request
        final ImageMetadataRequest imageMetadataRequest = ImageMetadataRequest.newBuilder()
            .addParam(ImageMgmtConstants.IMAGE_TYPE, imageType) // mandatory parameter
            .addParamIfPresent(ImageMgmtConstants.IMAGE_VERSION, imageVersion) // optional parameter
            .addParamIfPresent(ImageMgmtConstants.VERSION_STATE, versionState) // optional parameter
            .build();
        // invoke service method and get response in string format
        response =
            this.objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(
                this.imageVersionService.findImageVersions(imageMetadataRequest));
      }
      writeResponse(resp, response);
    } catch (final ImageMgmtInvalidPermissionException e) {
      log.error("Unable to get image version. Invalid permission.", e);
      sendErrorResponse(resp, HttpServletResponse.SC_FORBIDDEN,
          "Unable to get image version. Reason: " + e.getMessage());
    } catch (final Exception e) {
      log.error("Requested image metadata not found " + e);
      sendErrorResponse(resp, HttpServletResponse.SC_NOT_FOUND,
          "Requested image metadata not found. Reason : " + e.getMessage());
    }
  }

  @Override
  protected void handlePost(
      final HttpServletRequest req, final HttpServletResponse resp, final Session session)
      throws ServletException, IOException {
    final Map<String, String> templateVariableToValue = new HashMap<>();
    if (BASE_IMAGE_VERSION_URI.equals(req.getRequestURI())) {
      handleCreateImageVersion(req, resp, session);
    } else if (SINGLE_IMAGE_VERSION_URI_TEMPLATE.match(req.getRequestURI(),
        templateVariableToValue)) {
      handleUpdateImageVersion(req, resp, session, templateVariableToValue);
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
      final String jsonPayload = HttpRequestUtils.getBody(req);
      // Check for required permission to invoke the API
      final String imageType = JSONUtils.extractTextFieldValueFromJsonString(jsonPayload, IMAGE_TYPE);
      if (!hasImageManagementPermission(imageType, session.getUser(), Type.CREATE)) {
        log.debug(String.format("Invalid permission to create image version "
            + "for user: %s image type: %s.", session.getUser().getUserId(), imageType));
        throw new ImageMgmtInvalidPermissionException("Invalid permission to create image version");
      }
      // Build ImageMetadataRequest DTO to transfer the input request
      final ImageMetadataRequest imageMetadataRequest = ImageMetadataRequest.newBuilder()
          .jsonPayload(jsonPayload)
          .user(session.getUser().getUserId())
          .build();
      // Create image version metadata and image version id
      final Integer imageVersionId = this.imageVersionService.createImageVersion(imageMetadataRequest);
      // prepare to send response
      resp.setStatus(HttpStatus.SC_CREATED);
      resp.setHeader("Location",
          SINGLE_IMAGE_VERSION_URI_TEMPLATE.createURI(imageVersionId.toString()));
      sendResponse(resp, HttpServletResponse.SC_CREATED, new HashMap<>());
    } catch (final ImageMgmtValidationException e) {
      log.error("Input for creating image version metadata is invalid", e);
      sendErrorResponse(resp, HttpServletResponse.SC_BAD_REQUEST,
          "Bad request for creating image version metadata");
    } catch (final ImageMgmtInvalidPermissionException e) {
      log.error("Unable to create image version. Invalid permission.", e);
      sendErrorResponse(resp, HttpServletResponse.SC_FORBIDDEN,
          "Unable to create image version. Reason: " + e.getMessage());
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
      final String idString = templateVariableToValue.get(IMAGE_VERSION_ID_KEY);
      final Integer id = Ints.tryParse(idString);
      if (id == null) {
        log.error("Invalid image version id: ", idString);
        throw new ImageMgmtValidationException("Image version id is invalid");
      }
      final String jsonPayload = HttpRequestUtils.getBody(req);
      // Check for required permission to invoke the API
      final String imageType = JSONUtils.extractTextFieldValueFromJsonString(jsonPayload, IMAGE_TYPE);
      if (!hasImageManagementPermission(imageType, session.getUser(), Type.UPDATE)) {
        log.debug(String.format("Invalid permission to update image version "
            + "for user: %s image type: %s.", session.getUser().getUserId(), imageType));
        throw new ImageMgmtInvalidPermissionException("Invalid permission to update image version");
      }
      // Build ImageMetadataRequest DTO to transfer the input request
      final ImageMetadataRequest imageMetadataRequest = ImageMetadataRequest.newBuilder()
          .jsonPayload(jsonPayload)
          .user(session.getUser().getUserId())
          .addParam(ID_KEY, id)
          .build();
      // Create image version metadata and image version id
      this.imageVersionService.updateImageVersion(imageMetadataRequest);
      sendResponse(resp, HttpServletResponse.SC_OK, new HashMap<>());
    } catch (final ImageMgmtValidationException e) {
      log.error("Input for updating image version metadata is invalid", e);
      sendErrorResponse(resp, HttpServletResponse.SC_BAD_REQUEST,
          "Bad request for updating image version metadata");
    } catch (final ImageMgmtInvalidPermissionException e) {
      log.error("Unable to update image version. Invalid permission.", e);
      sendErrorResponse(resp, HttpServletResponse.SC_FORBIDDEN,
          "Unable to update image version. Reason: " + e.getMessage());
    } catch (final Exception e) {
      log.error("Exception while updating image version metadata", e);
      sendErrorResponse(resp, HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
          "Exception while updating image version metadata. " + e.getMessage());
    }
  }

}
