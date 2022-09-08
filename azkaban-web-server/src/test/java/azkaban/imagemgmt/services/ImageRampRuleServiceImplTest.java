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
import azkaban.imagemgmt.daos.ImageTypeDaoImpl;
import azkaban.imagemgmt.daos.ImageVersionDao;
import azkaban.imagemgmt.daos.ImageVersionDaoImpl;
import azkaban.imagemgmt.daos.RampRuleDao;
import azkaban.imagemgmt.daos.RampRuleDaoImpl;
import azkaban.imagemgmt.dto.ImageRampRuleRequestDTO;
import azkaban.imagemgmt.dto.RampRuleOwnershipRequestDTO;
import azkaban.imagemgmt.models.ImageOwnership;
import azkaban.imagemgmt.models.ImageType;
import azkaban.imagemgmt.permission.PermissionManager;
import azkaban.imagemgmt.permission.PermissionManagerImpl;
import azkaban.imagemgmt.utils.ConverterUtils;
import azkaban.user.Permission;
import azkaban.user.Role;
import azkaban.user.User;
import azkaban.user.UserManager;
import azkaban.user.XmlUserManager;
import azkaban.utils.JSONUtils;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;


public class ImageRampRuleServiceImplTest {
  private RampRuleDao _rampRuleDao;
  private ImageTypeDao _imageTypeDao;
  private ImageVersionDao _imageVersionDao;
  private UserManager _userManager;
  private ImageRampRuleService _rampRuleService;
  private PermissionManager _permissionManager;

  private ConverterUtils _converterUtils;

  @Before
  public void setup() {
    this._rampRuleDao = mock(RampRuleDaoImpl.class);
    this._imageTypeDao = mock(ImageTypeDaoImpl.class);
    this._imageVersionDao = mock(ImageVersionDaoImpl.class);
    this._userManager = mock(XmlUserManager.class);
    this._permissionManager = new PermissionManagerImpl(_imageTypeDao, _userManager);
    this._rampRuleService =
        new ImageRampRuleServiceImpl(_rampRuleDao, _imageTypeDao, _imageVersionDao, _permissionManager);
    this._converterUtils = new ConverterUtils(new ObjectMapper());
  }

  @Test
  public void testCreateRule() {
    User user = new User("testUser");
    final String json = JSONUtils.readJsonFileAsString("image_management/image_ramp_rule.json");
    final ImageRampRuleRequestDTO requestDTO = _converterUtils.convertToDTO(json, ImageRampRuleRequestDTO.class);
    when(_imageTypeDao.getImageTypeByName(requestDTO.getImageName())).thenReturn(Optional.of(new ImageType()));
    when(_imageVersionDao.isInvalidVersion(requestDTO.getImageName(), requestDTO.getImageVersion())).thenReturn(true);
    ImageOwnership ownership = new ImageOwnership();
    Role role = new Role(user.getUserId(), new Permission(Permission.Type.ADMIN));
    user.addRole("admin");
    ownership.setOwner(user.getUserId());
    ownership.setRole(ImageOwnership.Role.ADMIN);
    when(_imageTypeDao.getImageTypeOwnership(requestDTO.getImageName())).thenReturn(Collections.singletonList(ownership));

    when(_userManager.getRole(any())).thenReturn(role);
    when(_userManager.validateUserGroupMembership(any(), any())).thenReturn(true);
    when(_userManager.validateGroup(any())).thenReturn(true);
    when(_rampRuleDao.addRampRule(any())).thenReturn(1);
    assertThatCode(() -> _rampRuleService.createRule(requestDTO, user)).doesNotThrowAnyException();
  }

  @Test
  public void testCreateHPFlowRule() {
    User user = new User("testUser");
    final String json = JSONUtils.readJsonFileAsString("image_management/hp_flow_rule.json");
    final RampRuleOwnershipRequestDTO requestDTO = _converterUtils.convertToDTO(json, RampRuleOwnershipRequestDTO.class);
    ImageOwnership ownership = new ImageOwnership();
    Role role = new Role(user.getUserId(), new Permission(Permission.Type.ADMIN));
    user.addRole("admin");
    ownership.setOwner(user.getUserId());
    ownership.setRole(ImageOwnership.Role.ADMIN);

    when(_userManager.getRole(any())).thenReturn(role);
    when(_userManager.validateUserGroupMembership(any(), any())).thenReturn(true);
    when(_userManager.validateGroup(any())).thenReturn(true);
    when(_rampRuleDao.addRampRule(any())).thenReturn(1);
    assertThatCode(() -> _rampRuleService.createHpFlowRule(requestDTO, user)).doesNotThrowAnyException();
  }

  @Test
  public void testUpdateRuleOwner() {
    User user = new User("testUser");
    final String json = JSONUtils.readJsonFileAsString("image_management/hp_flow_rule.json");
    final RampRuleOwnershipRequestDTO requestDTO = _converterUtils.convertToDTO(json, RampRuleOwnershipRequestDTO.class);
    ImageOwnership ownership = new ImageOwnership();
    Role role = new Role(user.getUserId(), new Permission(Permission.Type.ADMIN));
    user.addRole("admin");
    ownership.setOwner(user.getUserId());
    ownership.setRole(ImageOwnership.Role.ADMIN);

    Set<String> existingOwners = new HashSet<>();
    existingOwners.add("testUser");
    when(_rampRuleDao.getOwners(requestDTO.getRuleName())).thenReturn(existingOwners);

    when(_userManager.getRole(any())).thenReturn(role);
    when(_userManager.validateGroup(any())).thenReturn(true);
    when(_rampRuleDao.updateOwnerships(any(), any(), any())).thenReturn(1);
    assertThatCode(() -> _rampRuleService.updateOwnership(requestDTO, user, ImageRampRuleService.OperationType.ADD))
        .doesNotThrowAnyException();
  }

  @Test
  public void testInvalidImageName() {
    final String json = JSONUtils.readJsonFileAsString("image_management/image_ramp_rule_invalid.json");
    final ImageRampRuleRequestDTO requestDTO = _converterUtils.convertToDTO(json, ImageRampRuleRequestDTO.class);
    when(_imageTypeDao.getImageTypeByName(requestDTO.getImageName())).thenReturn(Optional.empty());
    assertThatCode(() -> _rampRuleService.createRule(requestDTO, new User("testUser")))
        .hasMessageContaining("Invalid image type");
  }

  @Test
  public void testInvalidImageVersion() {
    final String json = JSONUtils.readJsonFileAsString("image_management/image_ramp_rule_invalid.json");
    final ImageRampRuleRequestDTO requestDTO = _converterUtils.convertToDTO(json, ImageRampRuleRequestDTO.class);
    when(_imageTypeDao.getImageTypeByName(requestDTO.getImageName())).thenReturn(Optional.of(new ImageType()));
    when(_imageVersionDao.isInvalidVersion(requestDTO.getImageName(), requestDTO.getImageVersion())).thenReturn(false);
    assertThatCode(() -> _rampRuleService.createRule(requestDTO, new User("testUser")))
        .hasMessageContaining("Invalid image version");
  }

  @Test
  public void testNotAuthorizedUser() {
    User user = new User("testUser");
    String group = "group";
    final String json = JSONUtils.readJsonFileAsString("image_management/image_ramp_rule_without_owners.json");
    final ImageRampRuleRequestDTO requestDTO = _converterUtils.convertToDTO(json, ImageRampRuleRequestDTO.class);
    when(_imageTypeDao.getImageTypeByName(requestDTO.getImageName())).thenReturn(Optional.of(new ImageType()));
    when(_imageVersionDao.isInvalidVersion(requestDTO.getImageName(), requestDTO.getImageVersion())).thenReturn(true);
    ImageOwnership ownership = new ImageOwnership();
    ownership.setOwner(group);
    when(_imageTypeDao.getImageTypeOwnership(requestDTO.getImageName())).thenReturn(Collections.singletonList(ownership));
    when(_userManager.validateUserGroupMembership(any(), any())).thenReturn(false);
    assertThatCode(() -> _rampRuleService.createRule(requestDTO, user))
        .hasMessageContaining("unauthorized user");
  }


}
