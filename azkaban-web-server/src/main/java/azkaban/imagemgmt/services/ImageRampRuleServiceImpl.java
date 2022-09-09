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
import azkaban.imagemgmt.dto.RampRuleOwnershipDTO;
import azkaban.imagemgmt.dto.ImageRampRuleRequestDTO;
import azkaban.imagemgmt.exception.ErrorCode;
import azkaban.imagemgmt.exception.ImageMgmtInvalidInputException;
import azkaban.imagemgmt.exception.ImageMgmtDaoException;
import azkaban.imagemgmt.exception.ImageMgmtInvalidPermissionException;
import azkaban.imagemgmt.exception.ImageMgmtValidationException;
import azkaban.imagemgmt.models.ImageRampRule;
import azkaban.imagemgmt.models.ImageType;
import azkaban.imagemgmt.permission.PermissionManager;
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

  @Inject
  public ImageRampRuleServiceImpl(final RampRuleDao rampRuleDao,
                                  final ImageTypeDao imageTypeDao,
                                  final ImageVersionDao imageVersionDao,
                                  final PermissionManager permissionManager) {
    this.rampRuleDao = rampRuleDao;
    this.imageTypeDao = imageTypeDao;
    this.imageVersionDao = imageVersionDao;
    this.permissionManager = permissionManager;
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
    if (!this.imageVersionDao.isInvalidVersion(rampRuleRequest.getImageName(), rampRuleRequest.getImageVersion())) {
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
   * @param hpFlowRuleRequestDTO
   * @param user
   * @throws ImageMgmtDaoException when DB insertion fail
   * @throws ImageMgmtValidationException when user does not have permission
   * */
  @Override
  public void createHpFlowRule(final RampRuleOwnershipDTO hpFlowRuleRequestDTO, final User user) {
    if (hpFlowRuleRequestDTO.getOwnerships() == null || hpFlowRuleRequestDTO.getOwnerships().isEmpty()) {
      throw new ImageMgmtInvalidInputException(ErrorCode.BAD_REQUEST,
          "missing ownerships, please specify valid ldap user");
    }
    List<String> ruleOwners = Arrays.asList(hpFlowRuleRequestDTO.getOwnerships().split(","));
    permissionManager.validateIdentity(ruleOwners);
    ImageRampRule rampRule = new ImageRampRule.Builder()
        .setRuleName(hpFlowRuleRequestDTO.getRuleName())
        .setOwners(new HashSet<>(ruleOwners))
        .setHPRule(true)
        .setCreatedBy(hpFlowRuleRequestDTO.getCreatedBy())
        .setModifiedBy(hpFlowRuleRequestDTO.getModifiedBy())
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
  public String updateOwnership(final RampRuleOwnershipDTO ruleOwnershipDTO, final User user,
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

  @Override
  public void deleteRule(final String ruleName) {

  }

  @Override
  public void addFlowsToRule(final List<String> flowIds, final String ruleName) {

  }

  @Override
  public void updateVersionOnRule(final String newVersion, final String ruleName) {

  }
}
