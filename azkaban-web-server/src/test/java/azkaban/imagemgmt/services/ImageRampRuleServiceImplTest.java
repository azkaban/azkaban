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
import azkaban.imagemgmt.dto.RampRuleFlowsDTO;
import azkaban.imagemgmt.dto.RampRuleOwnershipDTO;
import azkaban.imagemgmt.models.ImageOwnership;
import azkaban.imagemgmt.models.ImageRampRule;
import azkaban.imagemgmt.models.ImageType;
import azkaban.imagemgmt.permission.PermissionManager;
import azkaban.imagemgmt.permission.PermissionManagerImpl;
import azkaban.imagemgmt.utils.ConverterUtils;
import azkaban.project.JdbcProjectImpl;
import azkaban.project.ProjectLoader;
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
  private ProjectLoader _projectLoader;

  private ConverterUtils _converterUtils;

  @Before
  public void setup() {
    this._rampRuleDao = mock(RampRuleDaoImpl.class);
    this._imageTypeDao = mock(ImageTypeDaoImpl.class);
    this._imageVersionDao = mock(ImageVersionDaoImpl.class);
    this._userManager = mock(XmlUserManager.class);
    this._permissionManager = new PermissionManagerImpl(_imageTypeDao, _userManager);
    this._projectLoader = mock(JdbcProjectImpl.class);
    this._rampRuleService =
        new ImageRampRuleServiceImpl(_rampRuleDao, _imageTypeDao, _imageVersionDao, _permissionManager,
            _projectLoader);
    this._converterUtils = new ConverterUtils(new ObjectMapper());
  }

  @Test
  public void testCreateRule() {
    User user = new User("testUser");
    final String json = JSONUtils.readJsonFileAsString("image_management/image_ramp_rule.json");
    final ImageRampRuleRequestDTO requestDTO = _converterUtils.convertToDTO(json, ImageRampRuleRequestDTO.class);
    when(_imageTypeDao.getImageTypeByName(requestDTO.getImageName())).thenReturn(Optional.of(new ImageType()));
    when(_imageVersionDao.isInvalidVersion(requestDTO.getImageName(), requestDTO.getImageVersion())).thenReturn(false);
    ImageOwnership ownership = new ImageOwnership();
    Role role = new Role(user.getUserId(), new Permission(Permission.Type.ADMIN));
    user.addRole("admin");
    ownership.setOwner(user.getUserId());
    ownership.setRole(ImageOwnership.Role.ADMIN);
    when(_imageTypeDao.getImageTypeOwnership(requestDTO.getImageName())).thenReturn(Collections.singletonList(ownership));

    when(_userManager.getRole(any())).thenReturn(role);
    when(_userManager.validateUserGroupMembership(any(), any())).thenReturn(true);
    when(_userManager.validateLdapGroup(any())).thenReturn(true);
    when(_rampRuleDao.addRampRule(any())).thenReturn(1);
    assertThatCode(() -> _rampRuleService.createRule(requestDTO, user)).doesNotThrowAnyException();
  }

  @Test
  public void testCreateHPFlowRule() {
    User user = new User("testUser");
    final String json = JSONUtils.readJsonFileAsString("image_management/hp_flow_rule.json");
    final RampRuleOwnershipDTO requestDTO = _converterUtils.convertToDTO(json, RampRuleOwnershipDTO.class);
    ImageOwnership ownership = new ImageOwnership();
    Role role = new Role(user.getUserId(), new Permission(Permission.Type.ADMIN));
    user.addRole("admin");
    ownership.setOwner(user.getUserId());
    ownership.setRole(ImageOwnership.Role.ADMIN);

    when(_userManager.getRole(any())).thenReturn(role);
    when(_userManager.validateUserGroupMembership(any(), any())).thenReturn(true);
    when(_userManager.validateLdapGroup(any())).thenReturn(true);
    when(_rampRuleDao.addRampRule(any())).thenReturn(1);
    assertThatCode(() -> _rampRuleService.createHpFlowRule(requestDTO, user)).doesNotThrowAnyException();
  }

  @Test
  public void testAddRuleOwners() {
    User user = new User("testAdminUser");
    final String json = JSONUtils.readJsonFileAsString("image_management/flow_owners_update.json");
    final RampRuleOwnershipDTO requestDTO = _converterUtils.convertToDTO(json, RampRuleOwnershipDTO.class);
    ImageOwnership ownership = new ImageOwnership();
    Role role = new Role(user.getUserId(), new Permission(Permission.Type.ADMIN));
    user.addRole("admin");
    ownership.setOwner(user.getUserId());
    ownership.setRole(ImageOwnership.Role.ADMIN);
    when(_userManager.getRole(any())).thenReturn(role);
    when(_userManager.validateLdapGroup(any())).thenReturn(true);

    String existingOwner1 = "user2";
    String existingOwner2 = "user3";
    Set<String> existingOwners = new HashSet<>();
    existingOwners.add(existingOwner1);
    existingOwners.add(existingOwner2);
    when(_rampRuleDao.getOwners(requestDTO.getRuleName())).thenReturn(existingOwners);
    when(_rampRuleDao.updateOwnerships(any(), any(), any())).thenReturn(1);

    String updatedOwner = _rampRuleService.updateRuleOwnership(requestDTO, user, ImageRampRuleService.OperationType.ADD);
    assertThat(updatedOwner).contains(existingOwner1, existingOwner2);
    assertThat(updatedOwner).contains("user1");
  }

  @Test
  public void testRemoveRuleOwners() {
    User user = new User("testAdminUser");
    final String json = JSONUtils.readJsonFileAsString("image_management/flow_owners_update.json");
    final RampRuleOwnershipDTO requestDTO = _converterUtils.convertToDTO(json, RampRuleOwnershipDTO.class);
    ImageOwnership ownership = new ImageOwnership();
    Role role = new Role(user.getUserId(), new Permission(Permission.Type.ADMIN));
    user.addRole("admin");
    ownership.setOwner(user.getUserId());
    ownership.setRole(ImageOwnership.Role.ADMIN);
    when(_userManager.getRole(any())).thenReturn(role);
    when(_userManager.validateLdapGroup(any())).thenReturn(true);

    String existingOwner1 = "user2";
    String existingOwner2 = "user3";
    Set<String> existingOwners = new HashSet<>();
    existingOwners.add(existingOwner1);
    existingOwners.add(existingOwner2);
    when(_rampRuleDao.getOwners(requestDTO.getRuleName())).thenReturn(existingOwners);
    when(_rampRuleDao.updateOwnerships(any(), any(), any())).thenReturn(1);

    String updatedOwner = _rampRuleService.updateRuleOwnership(requestDTO, user, ImageRampRuleService.OperationType.REMOVE);
    assertThat(updatedOwner).contains(existingOwner2);
    assertThat(updatedOwner).doesNotContain("user1");
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
    when(_imageVersionDao.isInvalidVersion(requestDTO.getImageName(), requestDTO.getImageVersion())).thenReturn(true);
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
    when(_imageVersionDao.isInvalidVersion(requestDTO.getImageName(), requestDTO.getImageVersion())).thenReturn(false);
    ImageOwnership ownership = new ImageOwnership();
    ownership.setOwner(group);
    when(_imageTypeDao.getImageTypeOwnership(requestDTO.getImageName())).thenReturn(Collections.singletonList(ownership));
    when(_userManager.validateUserGroupMembership(any(), any())).thenReturn(false);
    assertThatCode(() -> _rampRuleService.createRule(requestDTO, user))
        .hasMessageContaining("unauthorized user");
  }

  @Test
  public void testAddFlowsToRule() {
    final String json = JSONUtils.readJsonFileAsString("image_management/add_flows_to_rule.json");
    final RampRuleFlowsDTO requestDTO = _converterUtils.convertToDTO(json, RampRuleFlowsDTO.class);
    User user = new User("testAdminUser");
    user.addRole("admin");
    Role role = new Role(user.getUserId(), new Permission(Permission.Type.ADMIN));
    when(_userManager.getRole(any())).thenReturn(role);

    String existingOwner1 = "testAdminUser";
    Set<String> existingOwners = new HashSet<>();
    existingOwners.add(existingOwner1);
    when(_rampRuleDao.getOwners(requestDTO.getRuleName())).thenReturn(existingOwners);
    when(_projectLoader.isFlowInProject(any(), any())).thenReturn(true);
    assertThatCode(() -> _rampRuleService.addFlowsToRule(requestDTO.getFlowIds(), requestDTO.getRuleName(), user))
        .doesNotThrowAnyException();
  }

  @Test
  public void testAddFlowsToRuleInvalidFlowId() {
    final String json = JSONUtils.readJsonFileAsString("image_management/add_flows_to_rule_invalid.json");
    final RampRuleFlowsDTO requestDTO = _converterUtils.convertToDTO(json, RampRuleFlowsDTO.class);
    User user = new User("testAdminUser");
    user.addRole("admin");
    Role role = new Role(user.getUserId(), new Permission(Permission.Type.ADMIN));
    when(_userManager.getRole(any())).thenReturn(role);

    String existingOwner1 = "testAdminUser";
    Set<String> existingOwners = new HashSet<>();
    existingOwners.add(existingOwner1);
    when(_rampRuleDao.getOwners(requestDTO.getRuleName())).thenReturn(existingOwners);
    when(_projectLoader.isFlowInProject(any(), any())).thenReturn(false);
    assertThatCode(() -> _rampRuleService.addFlowsToRule(requestDTO.getFlowIds(), requestDTO.getRuleName(), user))
        .hasMessageContaining("either project or flow not exist or active");
  }

  @Test
  public void testAddFlowsToRuleNonExistedProject() {
    final String json = JSONUtils.readJsonFileAsString("image_management/add_flows_to_rule_invalid.json");
    final RampRuleFlowsDTO requestDTO = _converterUtils.convertToDTO(json, RampRuleFlowsDTO.class);
    User user = new User("testAdminUser");
    user.addRole("admin");
    Role role = new Role(user.getUserId(), new Permission(Permission.Type.ADMIN));
    when(_userManager.getRole(any())).thenReturn(role);

    String existingOwner1 = "testAdminUser";
    Set<String> existingOwners = new HashSet<>();
    existingOwners.add(existingOwner1);
    when(_rampRuleDao.getOwners(requestDTO.getRuleName())).thenReturn(existingOwners);
    when(_projectLoader.fetchProjectByName(any())).thenReturn(null);
    assertThatCode(() -> _rampRuleService.addFlowsToRule(requestDTO.getFlowIds(), requestDTO.getRuleName(), user))
        .hasMessageContaining("either project or flow not exist or active");
  }

  @Test
  public void testUpdateVersionOnHPRule() {
    String ruleName = "testRule";
    String imageName = "testImageName";
    String imageVersion = "0.0.1";
    User user = new User("testAdminUser");
    user.addRole("admin");
    Role role = new Role(user.getUserId(), new Permission(Permission.Type.ADMIN));
    when(_userManager.getRole(any())).thenReturn(role);

    String curUser = "testAdminUser";
    Set<String> owners = new HashSet<>();
    owners.add(curUser);
    ImageRampRule mockedRule = new ImageRampRule.Builder()
        .setHPRule(true)
        .setRuleName(ruleName)
        .setImageName(imageName)
        .setImageVersion(imageVersion)
        .setOwners(owners)
        .build();
    when(_rampRuleDao.getRampRule(ruleName)).thenReturn(mockedRule);
    assertThatCode(() -> _rampRuleService.updateVersionOnRule("0.0.2", ruleName, user))
        .hasMessageContaining("Can't update version on a HP flow rule");
  }

  @Test
  public void testUpdateVersionOnNormalRule() {
    String ruleName = "testRule";
    String imageName = "testImageName";
    String imageVersion = "0.0.1";
    User user = new User("testAdminUser");
    user.addRole("admin");
    Role role = new Role(user.getUserId(), new Permission(Permission.Type.ADMIN));
    when(_userManager.getRole(any())).thenReturn(role);

    String curUser = "testAdminUser";
    Set<String> owners = new HashSet<>();
    owners.add(curUser);
    ImageRampRule mockedRule = new ImageRampRule.Builder()
        .setHPRule(false)
        .setRuleName(ruleName)
        .setImageName(imageName)
        .setImageVersion(imageVersion)
        .setOwners(owners)
        .build();
    when(_rampRuleDao.getRampRule(ruleName)).thenReturn(mockedRule);
    assertThatCode(() -> _rampRuleService.updateVersionOnRule("0.0.2", ruleName, user))
        .doesNotThrowAnyException();
  }

  @Test
  public void testDeleteRampRule() {
    String ruleName = "testRule";
    String imageName = "testImageName";
    String imageVersion = "0.0.1";
    User user = new User("testAdminUser");
    user.addRole("admin");
    Role role = new Role(user.getUserId(), new Permission(Permission.Type.ADMIN));
    when(_userManager.getRole(any())).thenReturn(role);

    String curUser = "testAdminUser";
    Set<String> owners = new HashSet<>();
    owners.add(curUser);
    ImageRampRule mockedRule = new ImageRampRule.Builder()
        .setHPRule(false)
        .setRuleName(ruleName)
        .setImageName(imageName)
        .setImageVersion(imageVersion)
        .setOwners(owners)
        .build();
    when(_rampRuleDao.getRampRule(ruleName)).thenReturn(mockedRule);
    assertThatCode(() -> _rampRuleService.deleteRule(ruleName, user)).doesNotThrowAnyException();
  }
}
