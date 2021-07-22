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

import azkaban.Constants.ImageMgmtConstants;
import azkaban.imagemgmt.dto.ImageTypeDTO;
import azkaban.imagemgmt.exception.ErrorCode;
import azkaban.imagemgmt.exception.ImageMgmtException;
import azkaban.imagemgmt.exception.ImageMgmtInvalidInputException;
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
import java.util.List;
import java.util.Map;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This servlet exposes the REST APIs such as create, get etc. for image type. Below are the
 * supported APIs. Create Image Type API: POST /imageTypes?session.id=? --data @payload.json Get
 * Image Type API: GET /imageTypes?&imageType=? GET /imageTypes/{imageType} GET /imageTypes/{id} GET
 * /imageTypes
 */
public class ImageTypeServlet extends LoginAbstractAzkabanServlet {

  private static final String GET_IMAGE_TYPE_URI = "/imageTypes";
  private static final String IMAGE_TYPE_NAME_OR_ID = "imageTypeNameOrId";
  private static final String FORBIDDEN_USER_ERR_MSG =
      "The user does not have appropriate permissions to"
          + " access this endpoint";
  private static final String PATH_NOT_SUPPORTED =
      "The path provided is not supported by this API. Please check the documentation";
  private static final UriTemplate IMAGE_TYPE_NAME_OR_ID_URI_TEMPLATE = new UriTemplate(
      String.format("/imageTypes/{%s}", IMAGE_TYPE_NAME_OR_ID));
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
      if (IMAGE_TYPE_NAME_OR_ID_URI_TEMPLATE.match(req.getRequestURI(), templateVariableToValue)) {
        getImageTypeDTOByIdOrImageTypeName(resp, session, templateVariableToValue);
      } else if (GET_IMAGE_TYPE_URI.equals(req.getRequestURI())) {
        ImageTypeDTO imageTypeDTO;
        if (req.getQueryString().contains(ImageMgmtConstants.IMAGE_TYPE)) {
          final String imageTypeName = HttpRequestUtils
              .getParam(req, ImageMgmtConstants.IMAGE_TYPE);
          if (!hasImageManagementPermission(imageTypeName, session.getUser(), Type.GET)) {
            log.info(FORBIDDEN_USER_ERR_MSG);
            throw new ImageMgmtInvalidPermissionException(ErrorCode.FORBIDDEN,
                FORBIDDEN_USER_ERR_MSG);
          }
          imageTypeDTO = this.imageTypeService.findImageTypeWithOwnershipsByName(imageTypeName);
          sendResponse(resp, HttpServletResponse.SC_OK, imageTypeDTO);
        }
        getAllImageTypeDTOs(resp, session);
      } else {
        log.info(PATH_NOT_SUPPORTED);
        throw new ImageMgmtInvalidInputException(ErrorCode.NOT_FOUND, PATH_NOT_SUPPORTED);
      }
    } catch (final ImageMgmtInvalidPermissionException e) {
      log.error("The user provided does not have permissions to access the resource");
      resp.setStatus(HttpStatus.SC_FORBIDDEN);
      sendErrorResponse(resp, HttpServletResponse.SC_FORBIDDEN,
          "User does not have permissions. Error Message: " + e.getMessage());
    } catch (final ImageMgmtException e) {
      if (e.getErrorCode() != null) {
        log.error("An error has occurred");
        resp.setStatus(e.getErrorCode().getCode());
      } else {
        resp.setStatus(HttpStatus.SC_BAD_REQUEST);
      }
      sendErrorResponse(resp, resp.getStatus(),
          "Exception on GET call to /imageTypes. Reason: " + e.getMessage());
    } catch (final Exception e) {
      log.error("Content is likely not present " + e);
      resp.setStatus(HttpStatus.SC_NOT_FOUND);
      sendErrorResponse(resp, HttpServletResponse.SC_NOT_FOUND,
          "Exception on GET call to /imageTypes. Reason: " + e.getMessage());
    }
  }

  private void getImageTypeDTOByIdOrImageTypeName(
      final HttpServletResponse resp, final Session session,
      final Map<String, String> templateVariableToValue) throws IOException {
    ImageTypeDTO imageTypeDTO;
    String imageTypeIdOrName = templateVariableToValue.get(IMAGE_TYPE_NAME_OR_ID);
    if (!StringUtils.isNumeric(imageTypeIdOrName)) {

      imageTypeDTO = this.imageTypeService.findImageTypeWithOwnershipsByName(imageTypeIdOrName);
    } else {
      imageTypeDTO = this.imageTypeService.findImageTypeWithOwnershipsById(imageTypeIdOrName);
    }
    if (!hasImageManagementPermission(imageTypeDTO.getName(), session.getUser(), Type.GET)) {
      log.info(FORBIDDEN_USER_ERR_MSG);
      throw new ImageMgmtInvalidPermissionException(ErrorCode.FORBIDDEN, FORBIDDEN_USER_ERR_MSG);
    }
    sendResponse(resp, HttpServletResponse.SC_OK, imageTypeDTO);
  }

  private void getAllImageTypeDTOs(final HttpServletResponse resp, final Session session)
      throws IOException {
    if (!isAzkabanAdmin(session.getUser())) {
      throw new ImageMgmtInvalidPermissionException(ErrorCode.FORBIDDEN, FORBIDDEN_USER_ERR_MSG);
    }
    List<ImageTypeDTO> imageTypesDTO =
        this.imageTypeService.getAllImageTypesWithOwnerships();
    sendResponse(resp, HttpServletResponse.SC_OK, imageTypesDTO);
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
      if (imageType == null) {
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
