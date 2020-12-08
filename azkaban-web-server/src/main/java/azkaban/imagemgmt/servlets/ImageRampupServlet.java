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

import static azkaban.Constants.ImageMgmtConstants.IMAGE_TYPE;

import azkaban.imagemgmt.dto.ImageMetadataRequest;
import azkaban.imagemgmt.exeception.ImageMgmtException;
import azkaban.imagemgmt.exeception.ImageMgmtInvalidPermissionException;
import azkaban.imagemgmt.exeception.ImageMgmtValidationException;
import azkaban.imagemgmt.models.ImageRampupPlan;
import azkaban.imagemgmt.services.ImageRampupService;
import azkaban.server.HttpRequestUtils;
import azkaban.server.session.Session;
import azkaban.user.Permission.Type;
import azkaban.utils.JSONUtils;
import azkaban.webapp.AzkabanWebServer;
import azkaban.webapp.servlet.LoginAbstractAzkabanServlet;
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
 * This servlet exposes the REST APIs such as create, get and update rampup for image type. Below
 * are the supported APIs. Create Image Rampup API: POST /imageRampup?session.id=? --data
 *
 * @payload.json Get active rampup for an image type : GET /imageRampup/spark_job?session.id=?
 * Update Image Rampup API: POST /imageRampup/spark_job?session.id=? --data @payload.json
 */
public class ImageRampupServlet extends LoginAbstractAzkabanServlet {

  private static final String BASE_IMAGE_RAMPUP_URI = "/imageRampup";
  private static final String IMAGE_RAMPUP_PLAN_ID_KEY = "id";
  private static final UriTemplate CREATE_IMAGE_RAMPUP_URI_TEMPLATE = new UriTemplate(
      String.format("/imageRampup/{%s}", IMAGE_RAMPUP_PLAN_ID_KEY));
  private static final UriTemplate UPDATE_IMAGE_RAMPUP_URI_TEMPLATE = new UriTemplate(
      String.format("/imageRampup/{%s}", IMAGE_TYPE));
  private ImageRampupService imageRampupService;
  private ObjectMapper objectMapper;

  private static final Logger log = LoggerFactory.getLogger(ImageRampupServlet.class);

  public ImageRampupServlet() {
    super(new ArrayList<>());
  }

  @Override
  public void init(ServletConfig config) throws ServletException {
    super.init(config);
    AzkabanWebServer server = (AzkabanWebServer) getApplication();
    objectMapper = server.getObjectMapper();
    imageRampupService = server.getImageRampupService();
  }

  @Override
  protected void handleGet(HttpServletRequest req, HttpServletResponse resp, Session session)
      throws ServletException, IOException {
    /* Get specific record */
    try {
      String response = null;
      if (BASE_IMAGE_RAMPUP_URI.equals(req.getRequestURI())) {
        // imageType must present. If not present throws ImageMgmtValidationException
        String imageType = HttpRequestUtils.getParam(req, IMAGE_TYPE);
        if (imageType == null) {
          log.error("Image type can't be null. Must provide valid image type to get active rampup"
              + " plan");
          throw new ImageMgmtValidationException(
              "Image type can't be null. Must provide valid image type to get rampup plan.");
        }
        // Check for required permission to invoke the API
        if (!hasImageManagementPermission(imageType, session.getUser(), Type.GET)) {
          throw new ImageMgmtInvalidPermissionException("Invalid permission to get image rampup "
              + "plan");
        }

        // invoke service method and get response in string format
        Optional<ImageRampupPlan> imageRampupPlan =
            imageRampupService.getActiveRampupPlan(imageType);
        if (imageRampupPlan.isPresent()) {
          response = objectMapper.writerWithDefaultPrettyPrinter()
              .writeValueAsString(imageRampupPlan.get());
        } else {
          throw new ImageMgmtException(
              String.format("There is no active rampup plan found for image "
                  + "type: %s.", imageType));
        }
      }
      writeResponse(resp, response);
    } catch (ImageMgmtValidationException e) {
      log.error("Input for getting active rampup plan is invalid", e);
      sendErrorResponse(resp, HttpServletResponse.SC_BAD_REQUEST,
          "Bad request for getting active rampup plan");
    } catch (ImageMgmtInvalidPermissionException e) {
      log.error("Unable to get image rampup plan. Invalid permission.", e);
      sendErrorResponse(resp, HttpServletResponse.SC_FORBIDDEN,
          "Unable to get image rampup plan. Reason: " + e.getMessage());
    } catch (Exception e) {
      log.error("Requested image rampup not found " + e);
      sendErrorResponse(resp, HttpServletResponse.SC_NOT_FOUND,
          "Requested image rampup not found. Reason : " + e.getMessage());
    }
  }

  @Override
  protected void handlePost(HttpServletRequest req, HttpServletResponse resp, Session session)
      throws ServletException, IOException {
    Map<String, String> templateVariableToValue = new HashMap<>();
    if (BASE_IMAGE_RAMPUP_URI.equals(req.getRequestURI())) {
      handleCreateImageRampup(req, resp, session);
    } else if (UPDATE_IMAGE_RAMPUP_URI_TEMPLATE.match(req.getRequestURI(),
        templateVariableToValue)) {
      handleUpdateImageRampup(req, resp, session, templateVariableToValue);
    } else {
      // Unsupported route, return an error.
      log.error("Invalid route for imageVersions endpoint: " + req.getRequestURI());
      sendErrorResponse(resp, HttpServletResponse.SC_NOT_FOUND,
          "The requested resource was not found.");
    }
  }

  private void handleCreateImageRampup(HttpServletRequest req,
      HttpServletResponse resp, Session session) throws ServletException, IOException {
    try {
      String jsonPayload = HttpRequestUtils.getBody(req);
      // Check for required permission to invoke the API
      String imageType = JSONUtils.extractTextFieldValueFromJsonString(jsonPayload, IMAGE_TYPE);
      if (!hasImageManagementPermission(imageType, session.getUser(), Type.CREATE)) {
        throw new ImageMgmtInvalidPermissionException("Invalid permission to create image rampup "
            + "plan");
      }
      // Build ImageMetadataRequest DTO to transfer the input request
      ImageMetadataRequest imageMetadataRequest = ImageMetadataRequest.newBuilder()
          .jsonPayload(jsonPayload)
          .user(session.getUser().getUserId())
          .build();
      // Create image version metadata and image version id
      Integer imageRampupPlanId = imageRampupService.createImageRampupPlan(imageMetadataRequest);
      // prepare to send response
      resp.setStatus(HttpStatus.SC_CREATED);
      resp.setHeader("Location",
          CREATE_IMAGE_RAMPUP_URI_TEMPLATE.createURI(imageRampupPlanId.toString()));
      sendResponse(resp, HttpServletResponse.SC_CREATED, new HashMap<>());
    } catch (ImageMgmtValidationException e) {
      log.error("Input for creating image rampup plan is invalid", e);
      sendErrorResponse(resp, HttpServletResponse.SC_BAD_REQUEST,
          "Bad request for creating image rampup plan. " + e.getMessage());
    } catch (ImageMgmtInvalidPermissionException e) {
      log.error("Unable to create image rampup plan. Invalid permission.", e);
      sendErrorResponse(resp, HttpServletResponse.SC_FORBIDDEN,
          "Unable to create image rampup plan. Reason: " + e.getMessage());
    } catch (Exception e) {
      log.error("Exception while creating image rampup plan", e);
      sendErrorResponse(resp, HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
          "Exception while creating image rampup plan. " + e.getMessage());
    }
  }

  private void handleUpdateImageRampup(HttpServletRequest req,
      HttpServletResponse resp, Session session,
      Map<String, String> templateVariableToValue) throws ServletException,
      IOException {
    try {
      String imageType = templateVariableToValue.get(IMAGE_TYPE);
      if (imageType == null) {
        log.error("Image type can't be null. Must provide valid image type to update rampup.");
        throw new ImageMgmtValidationException(
            "Image type can't be null. Must provide valid image type to update rampup.");
      }
      // Check for required permission to invoke the API
      if (!hasImageManagementPermission(imageType, session.getUser(), Type.UPDATE)) {
        throw new ImageMgmtInvalidPermissionException("Invalid permission to update image rampup "
            + "plan");
      }
      String jsonPayload = HttpRequestUtils.getBody(req);
      // Build ImageMetadataRequest DTO to transfer the input request
      ImageMetadataRequest imageMetadataRequest = ImageMetadataRequest.newBuilder()
          .jsonPayload(jsonPayload)
          .user(session.getUser().getUserId())
          .addParam(IMAGE_TYPE, imageType)
          .build();
      // update image rampup details
      imageRampupService.updateImageRampupPlan(imageMetadataRequest);
      sendResponse(resp, HttpServletResponse.SC_OK, new HashMap<>());
    } catch (ImageMgmtValidationException e) {
      log.error("Input for updating image rampup metadata is invalid", e);
      sendErrorResponse(resp, HttpServletResponse.SC_BAD_REQUEST,
          "Bad request for updating image rampup metadata");
    } catch (ImageMgmtInvalidPermissionException e) {
      log.error("Unable to update image rampup plan. Invalid permission.", e);
      sendErrorResponse(resp, HttpServletResponse.SC_FORBIDDEN,
          "Unable to update image rampup plan. Reason: " + e.getMessage());
    } catch (Exception e) {
      log.error("Exception while updating image rampup metadata", e);
      sendErrorResponse(resp, HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
          "Exception while updating image rampup metadata. " + e.getMessage());
    }
  }
}
