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
package azkaban.imagemgmt.services;

import azkaban.imagemgmt.daos.ImageTypeDao;
import azkaban.imagemgmt.daos.ImageVersionDao;
import azkaban.imagemgmt.daos.RampRuleDao;
import azkaban.imagemgmt.dto.RampRuleFlowsDTO.ProjectFlow;
import azkaban.imagemgmt.dto.RampRuleOwnershipDTO;
import azkaban.imagemgmt.dto.ImageRampRuleRequestDTO;
import azkaban.imagemgmt.exception.ErrorCode;
import azkaban.imagemgmt.exception.ImageMgmtException;
import azkaban.imagemgmt.exception.ImageMgmtInvalidInputException;
import azkaban.imagemgmt.exception.ImageMgmtDaoException;
import azkaban.imagemgmt.exception.ImageMgmtInvalidPermissionException;
import azkaban.imagemgmt.exception.ImageMgmtValidationException;
import azkaban.imagemgmt.models.ImageRampRule;
import azkaban.imagemgmt.models.ImageType;
import azkaban.imagemgmt.permission.PermissionManager;
import azkaban.project.ProjectLoader;
import azkaban.project.ProjectManagerException;
import azkaban.user.User;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import joptsimple.internal.Strings;
import org.apache.log4j.Logger;

/**
 * Implementation for adding/updating/deleting operations on RampRule.
 * */
@Singleton
public class ImageRampRuleServiceImpl implements ImageRampRuleService {

  public static final Logger log = Logger.getLogger(ImageRampRuleServiceImpl.class);

  private final RampRuleDao rampRuleDao;
  private final ImageTypeDao imageTypeDao;
  private final ImageVersionDao imageVersionDao;
  private final PermissionManager permissionManager;
  private final ProjectLoader projectLoader;

  @Inject
  public ImageRampRuleServiceImpl(final RampRuleDao rampRuleDao,
                                  final ImageTypeDao imageTypeDao,
                                  final ImageVersionDao imageVersionDao,
                                  final PermissionManager permissionManager,
                                  final ProjectLoader projectLoader) {
    this.rampRuleDao = rampRuleDao;
    this.imageTypeDao = imageTypeDao;
    this.imageVersionDao = imageVersionDao;
    this.permissionManager = permissionManager;
    this.projectLoader = projectLoader;
  }

  /**
   * Create ramp rule converted from ramp rule request, validate image version and user permission.
   * Then call for {@link RampRuleDao} to insert the entry into DB.
   *
   * @param rampRuleRequest
   * @param ldapUser
   * @throws ImageMgmtInvalidInputException when failing on invalid image metadata
   * @throws ImageMgmtDaoException when DB insertion fail
   * @throws ImageMgmtInvalidPermissionException when user does not have permission
   * */
  @Override
  public void createRule(final ImageRampRuleRequestDTO rampRuleRequest, final User ldapUser){
    // validate image_name and image_version
    final ImageType imageType = imageTypeDao
      .getImageTypeByName(rampRuleRequest.getImageName())
        .orElseThrow(() -> new ImageMgmtInvalidInputException(ErrorCode.NOT_FOUND, String.format("Unable to"
            + " fetch image type metadata. Invalid image type: %s.", rampRuleRequest.getImageName())));
    if (this.imageVersionDao.isInvalidVersion(rampRuleRequest.getImageName(), rampRuleRequest.getImageVersion())) {
      log.error("fail to validate image version: " + rampRuleRequest.getImageVersion());
      throw new ImageMgmtInvalidInputException(ErrorCode.NOT_FOUND, String.format(
          "Unable to fetch image version metadata. Invalid image version: %s.", rampRuleRequest.getImageVersion()));
    }
    Set<String> ownerships;
    // if user does not specify owners of a normal ramp rule, use ImageType owners as default
    if (rampRuleRequest.getOwnerships() == null || rampRuleRequest.getOwnerships().isEmpty()) {
      // fetch ownerships from image_ownerships and validate user permission
      ownerships = permissionManager.validatePermissionAndGetOwnerships(imageType.getName(), ldapUser);
    } else {
      List<String> ruleOwners = Arrays.asList(rampRuleRequest.getOwnerships().split(","));
      permissionManager.validateIdentity(ruleOwners);
      ownerships = new HashSet<>(ruleOwners);
    }

    // convert ImageRampRule and insert new ramp rule into DB
    ImageRampRule rampRule = new ImageRampRule.Builder()
        .setRuleName(rampRuleRequest.getRuleName())
        .setImageName(rampRuleRequest.getImageName())
        .setImageVersion(rampRuleRequest.getImageVersion())
        .setOwners(ownerships)
        .setHPRule(false)
        .setCreatedBy(rampRuleRequest.getCreatedBy())
        .setModifiedBy(rampRuleRequest.getModifiedBy())
        .build();
    rampRuleDao.addRampRule(rampRule);
  }

  /**
   * Create HP Flow rule converted from HPFlowRule request, validate input ownerships.
   * Then call for {@link RampRuleDao} to insert the entry into DB.
   *
   * @param hpFlowRuleOwnershipRequestDTO
   * @param user
   * @throws ImageMgmtDaoException when DB insertion fail
   * @throws ImageMgmtValidationException when user does not have permission
   * */
  @Override
  public void createHpFlowRule(final RampRuleOwnershipDTO hpFlowRuleOwnershipRequestDTO, final User user) {
    if (hpFlowRuleOwnershipRequestDTO.getOwnerships() == null
        || hpFlowRuleOwnershipRequestDTO.getOwnerships().isEmpty()) {
      throw new ImageMgmtInvalidInputException(ErrorCode.BAD_REQUEST,
          "missing ownerships, please specify valid ldap user");
    }
    List<String> ruleOwners = Arrays.asList(hpFlowRuleOwnershipRequestDTO.getOwnerships().split(","));
    permissionManager.validateIdentity(ruleOwners);
    ImageRampRule rampRule = new ImageRampRule.Builder()
        .setRuleName(hpFlowRuleOwnershipRequestDTO.getRuleName())
        .setOwners(new HashSet<>(ruleOwners))
        .setHPRule(true)
        .setCreatedBy(hpFlowRuleOwnershipRequestDTO.getCreatedBy())
        .setModifiedBy(hpFlowRuleOwnershipRequestDTO.getModifiedBy())
        .build();
    rampRuleDao.addRampRule(rampRule);
  }

  /**
   * Update Ramp Rule ownership based on {@link RampRuleOwnershipDTO} from user request to add/remove owners,
   * generate new owner list and update at DB.
   * Only azkaban admin or existing owners has the permission.
   *
   * @param ruleOwnershipDTO DTO from requestBody
   * @param user
   * @param operationType Add/Remove owners
   * @throws ImageMgmtDaoException when DB update fail
   * @throws ImageMgmtValidationException when user does not have permission
   * @return newOwners */
  @Override
  public String updateRuleOwnership(final RampRuleOwnershipDTO ruleOwnershipDTO, final User user,
      final OperationType operationType) {
    Set<String> existingOwners = rampRuleDao.getOwners(ruleOwnershipDTO.getRuleName());
    // validate current user has permission to change owner
    if (!permissionManager.hasPermission(user, existingOwners)) {
      throw new ImageMgmtInvalidPermissionException(ErrorCode.UNAUTHORIZED,
          "current user "+ user.getUserId() + " does not have permission to change ownership");
    }
    // validate input ldaps
    List<String> deltaOwners = Arrays.asList(ruleOwnershipDTO.getOwnerships().split(","));
    permissionManager.validateIdentity(deltaOwners);
    switch (operationType) {
      case ADD:
        Set<String> missingLdaps = deltaOwners.stream()
          .filter(owner -> !existingOwners.contains(owner))
          .collect(Collectors.toSet());
        String newOwners = String.join(",", existingOwners).concat(",").concat(String.join(",", missingLdaps));
        rampRuleDao.updateOwnerships(newOwners, ruleOwnershipDTO.getRuleName(), ruleOwnershipDTO.getModifiedBy());
        return newOwners;
      case REMOVE:
        Set<String> alteredOwnership = existingOwners.stream()
            .filter(owner -> !deltaOwners.contains(owner)).collect(Collectors.toSet());
        String newOwnership = String.join(",", alteredOwnership);
        rampRuleDao.updateOwnerships(newOwnership, ruleOwnershipDTO.getRuleName(), ruleOwnershipDTO.getModifiedBy());
        return newOwnership;
    }
    return Strings.EMPTY;
  }

  /**
   * delete ramp rule's metadata based on given ruleName
   *
   * @param ruleName - ruleName in {@link ImageRampRule}
   * @param user - user to operate
   * */
  @Override
  public void deleteRule(final String ruleName, final User user) {
    // validate permission
    final Set<String> owners = rampRuleDao.getOwners(ruleName);
    if (!permissionManager.hasPermission(user, owners)) {
      log.error("current user "+ user.getUserId() + " does not have permission to delete ramp rule");
      throw new ImageMgmtInvalidPermissionException(ErrorCode.UNAUTHORIZED,
          "current user "+ user.getUserId() + " does not have permission to ramp rule");
    }
    rampRuleDao.deleteRampRule(ruleName);
  }

  /**
   * add flows into ramp rules. Validation will be performed based on owner list, active project and valid flows.
   * call Dao layer to insert flow to image deny metadata into DB.
   *
   * @param flowIds
   * @param ruleName
   * @param user
   * */
  @Override
  public void addFlowsToRule(final List<ProjectFlow> flowIds, final String ruleName, final User user) {
    // validate permission
    final Set<String> owners = rampRuleDao.getOwners(ruleName);
    if (!permissionManager.hasPermission(user, owners)) {
      log.error("current user "+ user.getUserId() + " does not have permission to add flows to Ramp rule");
      throw new ImageMgmtInvalidPermissionException(ErrorCode.UNAUTHORIZED,
          "current user "+ user.getUserId() + " does not have permission to add flows to Ramp rule");
    }
    // validate flowId is correct (valid and in right format)
    try {
      for (final ProjectFlow flowId : flowIds) {
        // validate flows and projects exist and flows are in the active project
        if (!projectLoader.isFlowInProject(flowId.projectName, flowId.flowName)) {
          log.error("flowId " + flowId + " invalid, either project or flow not exist or active.");
          throw new ImageMgmtInvalidInputException(ErrorCode.BAD_REQUEST,
              "flowId " + flowId + " invalid, either project or flow not exist or active.");
        }
      }
    } catch (ProjectManagerException e) {
      log.error("failed to validate inputs" + e);
      throw new ImageMgmtException(ErrorCode.BAD_REQUEST, "failed to validate inputs: " + e.getMessage());
    }

    // insert into flow.deny.list table with record {flowId, denyMode, denyVersions, ruleName}
    rampRuleDao.addFlowDenyInfo(flowIds, ruleName);
  }

  /**
   * Update normal ramp rule's version based on given ruleName, validated based on current user.
   *
   * @param ruleName - ruleName in {@link ImageRampRule}
   * @param newVersion - new version to be updated
   * @param user - user must have the permission to operate
   * @throws ImageMgmtException
   * */
  @Override
  public void updateVersionOnRule(final String newVersion, final String ruleName, final User user) {
    // validate permission
    final ImageRampRule imageRampRule = rampRuleDao.getRampRule(ruleName);
    final Set<String> owners = new HashSet<>(Arrays.asList(imageRampRule.getOwners().split(",")));
    if (!permissionManager.hasPermission(user, owners)) {
      log.error("current user "+ user.getUserId() + " does not have permission to add flows to Ramp rule");
      throw new ImageMgmtInvalidPermissionException(ErrorCode.UNAUTHORIZED,
          "current user "+ user.getUserId() + " does not have permission to add flows to Ramp rule");
    }
    if (imageRampRule.isHPRule()) {
      log.error("Can't update version on a HP flow rule");
      throw new ImageMgmtInvalidInputException(ErrorCode.BAD_REQUEST, "Can't update version on a HP flow rule");
    }
    if (this.imageVersionDao.isInvalidVersion(imageRampRule.getImageName(), newVersion)) {
      log.error("fail to validate image version: " + newVersion);
      throw new ImageMgmtInvalidInputException(ErrorCode.NOT_FOUND, String.format(
          "Unable to fetch image version metadata. Invalid image version: %s.", newVersion));
    }

    rampRuleDao.updateVersionOnRule(newVersion, ruleName, user.getUserId());
  }
}
