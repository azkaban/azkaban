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

import azkaban.imagemgmt.dto.ImageTypeDTO;
import azkaban.imagemgmt.exception.ErrorCode;
import azkaban.imagemgmt.exception.ImageMgmtException;
import azkaban.imagemgmt.exception.ImageMgmtInvalidPermissionException;
import azkaban.imagemgmt.exception.ImageMgmtValidationException;
import azkaban.imagemgmt.services.ImageTypeService;
import azkaban.imagemgmt.utils.ConverterUtils;
import azkaban.server.HttpRequestUtils;
import azkaban.server.session.Session;
import azkaban.user.Permission.Type;
import azkaban.webapp.AzkabanWebServer;
import azkaban.webapp.servlet.LoginAbstractAzkabanServlet;
import com.linkedin.jersey.api.uri.UriTemplate;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This servlet exposes the REST APIs such as create, get etc. for image type. Below are the
 * supported APIs. Create Image Type API: POST /imageTypes?session.id=? --data @payload.json Get
 * Image Type API: GET /imageTypes?session.id=?&imageType=? GET /imageTypes/{id}?session.id=?
 */
public class ImageTypeServlet extends LoginAbstractAzkabanServlet {

  private static final String GET_IMAGE_TYPE_URI = "/imageTypes";
  private static final String IMAGE_TYPE_ID_KEY = "id";
  private static final UriTemplate IMAGE_TYPE_WITH_ID_URI_TEMPLATE = new UriTemplate(
      String.format("/imageTypes/{%s}", IMAGE_TYPE_ID_KEY));
  private ImageTypeService imageTypeService;
  private ConverterUtils converterUtils;

  private static final Logger log = LoggerFactory.getLogger(ImageTypeServlet.class);

  public ImageTypeServlet() {
    super(new ArrayList<>());
  }

  @Override
  public void init(final ServletConfig config) throws ServletException {
    super.init(config);
    final AzkabanWebServer server = (AzkabanWebServer) getApplication();
    this.imageTypeService = server.getImageTypeService();
    this.converterUtils = server.getConverterUtils();
  }

  @Override
  protected void handleGet(
      final HttpServletRequest req, final HttpServletResponse resp, final Session session)
      throws ServletException, IOException {
    try {
      final Map<String, String> templateVariableToValue = new HashMap<>();
      if (IMAGE_TYPE_WITH_ID_URI_TEMPLATE.match(req.getRequestURI(), templateVariableToValue)) {
        // TODO: Implementation will be provided in the future PR
      } else if (GET_IMAGE_TYPE_URI.equals(req.getRequestURI())) {
        // TODO: Implementation will be provided in the future PR
      }
    } catch (final Exception e) {
      log.error("Content is likely not present " + e);
      resp.setStatus(HttpStatus.SC_NOT_FOUND);
    }
  }

  @Override
  protected void handlePost(
      final HttpServletRequest req, final HttpServletResponse resp, final Session session)
      throws ServletException, IOException {
    try {
      final String jsonPayload = HttpRequestUtils.getBody(req);
      // Convert to GenericImageType DTO to transfer the input request
      final ImageTypeDTO genericImageType = this.converterUtils.convertToDTO(jsonPayload,
          ImageTypeDTO.class);
      // Check for required permission to invoke the API
      final String imageType = genericImageType.getName();
      if(imageType == null) {
        log.info("Required field imageType is null. Must provide valid imageType to "
            + "create/register image type.");
        throw new ImageMgmtValidationException(ErrorCode.BAD_REQUEST, "Required field imageType is"
            + " null. Must provide valid imageType to create/register image type.");
      }
      if (!hasImageManagementPermission(imageType, session.getUser(), Type.CREATE)) {
        log.debug(String.format("Invalid permission to create image type for "
            + "user: %s, image type: %s.", session.getUser().getUserId(), imageType));
        throw new ImageMgmtInvalidPermissionException(ErrorCode.FORBIDDEN, "Invalid permission to "
            + "create image type");
      }

      genericImageType.setCreatedBy(session.getUser().getUserId());
      genericImageType.setModifiedBy(session.getUser().getUserId());
      // Create image type and get image type id
      final Integer imageTypeId = this.imageTypeService.createImageType(genericImageType);
      // prepare to send response
      resp.setStatus(HttpStatus.SC_CREATED);
      resp.setHeader("Location",
          IMAGE_TYPE_WITH_ID_URI_TEMPLATE.createURI(imageTypeId.toString()));
      sendResponse(resp, HttpServletResponse.SC_CREATED, new HashMap<>());
    } catch (final ImageMgmtException e) {
      log.error("Exception while creating an image type", e);
      sendErrorResponse(resp, e.getErrorCode().getCode(), e.getMessage());
    } catch (final Exception e) {
      log.error("Exception while creating an image type", e);
      sendErrorResponse(resp, HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
          "Exception while creating an image type. Reason: " + e.getMessage());
    }
  }
}
