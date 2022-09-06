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
import azkaban.imagemgmt.dto.ImageRampRuleRequestDTO;
import azkaban.imagemgmt.exception.ErrorCode;
import azkaban.imagemgmt.exception.ImageMgmtInvalidInputException;
import azkaban.imagemgmt.exception.ImageMgmtDaoException;
import azkaban.imagemgmt.exception.ImageMgmtInvalidPermissionException;
import azkaban.imagemgmt.models.ImageRampRule;
import azkaban.imagemgmt.models.ImageType;
import azkaban.imagemgmt.permission.PermissionManager;
import azkaban.user.User;
import java.util.List;
import java.util.Set;
import javax.inject.Inject;
import javax.inject.Singleton;
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
   * Create ramp rule converted from ramp rule request, validate rampRuleRequest and user permission.
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
    // fetch ownerships from image_ownerships and validate user permission
    Set<String> imageOwnerships = permissionManager.validatePermissionAndGetOwnerships(imageType.getName(), ldapUser);

    // convert ImageRampRule and insert new ramp rule into DB
    ImageRampRule rampRule = new ImageRampRule.Builder()
        .setRuleName(rampRuleRequest.getRuleName())
        .setImageName(rampRuleRequest.getImageName())
        .setImageVersion(rampRuleRequest.getImageVersion())
        .setOwners(imageOwnerships)
        .setHPRule(false)
        .setCreatedBy(rampRuleRequest.getCreatedBy())
        .setModifiedBy(rampRuleRequest.getModifiedBy())
        .build();
    rampRuleDao.addRampRule(rampRule);
  }

  @Override
  public void createHpFlowRule(final ImageRampRule rule) {

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
