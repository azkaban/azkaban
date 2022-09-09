/*
 * Copyright 2022 LinkedIn Corp.
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

import azkaban.imagemgmt.dto.RampRuleOwnershipDTO;
import azkaban.imagemgmt.dto.ImageRampRuleRequestDTO;
import azkaban.imagemgmt.exception.ImageMgmtException;
import azkaban.imagemgmt.services.ImageRampRuleService;
import azkaban.imagemgmt.utils.ConverterUtils;
import azkaban.server.HttpRequestUtils;
import azkaban.server.session.Session;
import azkaban.user.User;
import azkaban.webapp.AzkabanWebServer;
import azkaban.webapp.servlet.LoginAbstractAzkabanServlet;
import java.io.IOException;
import java.util.ArrayList;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This servlet exposes the REST APIs such as create, delete and update rampup for image type.
 * Currently only supports:
 * Create Image Ramp Rule API: POST /imageRampRule/createRule
 * Create Image Ramp Rule API for HP flows: POST /imageRampRule/createHPFlowRule
 * Add managed flows into rule: POST /imageRampRule/addFlowsToRule
 * Modify image version on Rule: POST /imageRampRule/updateVersionOnRule
 * Delete rule: POST /imageRampRule/deleteRule
 * Modify ownerships: POST /imageRampRule/addOwners or POST /imageRampRule/removeOwners
 */
public class ImageRampRuleServlet extends LoginAbstractAzkabanServlet {
  private static final Logger LOG = LoggerFactory.getLogger(ImageRampRuleServlet.class);

  private ImageRampRuleService imageRampRuleService;
  private ConverterUtils utils;

  private final static String CREATE_RULE_URI = "/imageRampRule/createRule";
  private final static String CREATE_HP_FLOW_RULE_URI = "/imageRampRule/createHPFlowRule";
  private final static String ADD_OWNERS_URI = "/imageRampRule/addOwners";
  private final static String REMOVE_OWNERS_URI = "/imageRampRule/removeOwners";

  public ImageRampRuleServlet() {
    super(new ArrayList<>());
  }

  @Override
  public void init(final ServletConfig config) throws ServletException {
    super.init(config);
    final AzkabanWebServer server = (AzkabanWebServer) getApplication();
    this.imageRampRuleService = server.getImageRampRuleService();
    this.utils = server.getConverterUtils();
  }

  @Override
  protected void handleGet(HttpServletRequest req, HttpServletResponse resp, Session session)
      throws ServletException, IOException {

  }

  @Override
  protected void handlePost(HttpServletRequest req, HttpServletResponse resp, Session session)
      throws ServletException, IOException {
      String requestURI = req.getRequestURI();
      LOG.info("handle request from post uri: " + requestURI);
      User user = session.getUser();
      switch (requestURI) {
        case CREATE_RULE_URI:
          handleCreateRampRule(req, resp, user);
          break;
        case CREATE_HP_FLOW_RULE_URI:
          handleCreateHPFlowRule(req, resp, user);
          break;
        case ADD_OWNERS_URI:
          handleUpdateOwnerships(req, resp, user, ImageRampRuleService.OperationType.ADD);
          break;
        case REMOVE_OWNERS_URI:
          handleUpdateOwnerships(req, resp, user, ImageRampRuleService.OperationType.REMOVE);
          break;
      }

  }

  /**
   * create an exclusive rule for a certain version of the image,
   * Successful call would return CREATED(201).
   *
   * @throws ImageMgmtException with different ErrorCode, and the detailed error message.
   **/
  private void handleCreateRampRule(final HttpServletRequest req,
                                    final HttpServletResponse resp,
                                    final User user)
                                    throws ServletException {
    String requestBody = HttpRequestUtils.getBody(req);
    ImageRampRuleRequestDTO rampRuleRequestDTO;
    try {
      // while converting to requestDTO, validation on json/required parameters would be performed.
      rampRuleRequestDTO = utils.convertToDTO(requestBody, ImageRampRuleRequestDTO.class);
      rampRuleRequestDTO.setCreatedBy(user.getUserId());
      rampRuleRequestDTO.setModifiedBy(user.getUserId());
      imageRampRuleService.createRule(rampRuleRequestDTO, user);
      resp.setStatus(HttpStatus.SC_CREATED);
    } catch (ImageMgmtException e) {
      LOG.error("failed to create a rampRule: " + requestBody);
      resp.setStatus(e.getErrorCode().getCode(), e.getMessage());
    }
  }

  /**
   * create an exclusive rule for High priority flows which will deny all image Ramp Versions,
   * Successful call would return CREATED(201).
   *
   * @throws ImageMgmtException with different ErrorCode, and the detailed error message.
   **/
  private void handleCreateHPFlowRule(final HttpServletRequest req,
                                      final HttpServletResponse resp,
                                      final User user)
                                      throws ServletException {
    String requestBody = HttpRequestUtils.getBody(req);
    RampRuleOwnershipDTO hpFlowRuleRequestDTO;
    try {
      // while converting to requestDTO, validation on json/required parameters would be performed.
      hpFlowRuleRequestDTO = utils.convertToDTO(requestBody, RampRuleOwnershipDTO.class);
      hpFlowRuleRequestDTO.setCreatedBy(user.getUserId());
      hpFlowRuleRequestDTO.setModifiedBy(user.getUserId());
      imageRampRuleService.createHpFlowRule(hpFlowRuleRequestDTO, user);
      resp.setStatus(HttpStatus.SC_CREATED);
    } catch (ImageMgmtException e) {
      LOG.error("failed to create a rampRule: " + requestBody);
      resp.setStatus(e.getErrorCode().getCode(), e.getMessage());
    }
  }

  /**
   * Add/Remove owners for a ramp rule.
   * Successful call would return OK(200).
   *
   * @throws ImageMgmtException with different ErrorCode, and the detailed error message.
   **/
  private void handleUpdateOwnerships(final HttpServletRequest req,
                                      final HttpServletResponse resp,
                                      final User user,
                                      final ImageRampRuleService.OperationType type)
                                      throws ServletException, IOException {
    String requestBody = HttpRequestUtils.getBody(req);
    RampRuleOwnershipDTO deltaOwnershipRequestDTO;
    try {
      // while converting to requestDTO, validation on json/required parameters would be performed.
      deltaOwnershipRequestDTO = utils.convertToDTO(requestBody, RampRuleOwnershipDTO.class);
      deltaOwnershipRequestDTO.setModifiedBy(user.getUserId());
      String updatedOwners = imageRampRuleService.updateRuleOwnership(deltaOwnershipRequestDTO, user, type);
      // prepare response
      RampRuleOwnershipDTO responseModel = new RampRuleOwnershipDTO();
      responseModel.setOwnerships(updatedOwners);
      responseModel.setRuleName(deltaOwnershipRequestDTO.getRuleName());
      sendResponse(resp, HttpServletResponse.SC_OK, responseModel);
    } catch (ImageMgmtException e) {
      LOG.error("failed to update ownerships: " + requestBody);
      resp.setStatus(e.getErrorCode().getCode(), e.getMessage());
    }
  }

}
