package azkaban.imagemgmt.services;

import azkaban.imagemgmt.daos.ImageTypeDao;
import azkaban.imagemgmt.daos.ImageTypeDaoImpl;
import azkaban.imagemgmt.daos.ImageVersionDao;
import azkaban.imagemgmt.daos.ImageVersionDaoImpl;
import azkaban.imagemgmt.daos.RampRuleDao;
import azkaban.imagemgmt.daos.RampRuleDaoImpl;
import azkaban.imagemgmt.dto.ImageRampRuleRequestDTO;
import azkaban.imagemgmt.models.ImageOwnership;
import azkaban.imagemgmt.models.ImageType;
import azkaban.imagemgmt.utils.ConverterUtils;
import azkaban.user.UserManager;
import azkaban.user.XmlUserManager;
import azkaban.utils.JSONUtils;
import java.util.Collections;
import java.util.Optional;
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
  private ConverterUtils _converterUtils;

  @Before
  public void setup() {
    this._rampRuleDao = mock(RampRuleDaoImpl.class);
    this._imageTypeDao = mock(ImageTypeDaoImpl.class);
    this._imageVersionDao = mock(ImageVersionDaoImpl.class);
    this._userManager = mock(XmlUserManager.class);
    this._rampRuleService = new ImageRampRuleServiceImpl(_rampRuleDao, _imageTypeDao, _imageVersionDao, _userManager);
    this._converterUtils = new ConverterUtils(new ObjectMapper());
  }

  @Test
  public void testCreateRule() {
    String user = "user1";
    final String json = JSONUtils.readJsonFileAsString("image_management/image_ramp_rule.json");
    final ImageRampRuleRequestDTO requestDTO = _converterUtils.convertToDTO(json, ImageRampRuleRequestDTO.class);
    when(_imageTypeDao.getImageTypeByName(requestDTO.getImageName())).thenReturn(Optional.of(new ImageType()));
    when(_imageVersionDao.isInvalidVersion(requestDTO.getImageName(), requestDTO.getImageVersion())).thenReturn(true);
    ImageOwnership ownership = new ImageOwnership();
    ownership.setOwner(user);
    when(_imageTypeDao.getImageTypeOwnership(requestDTO.getImageName())).thenReturn(Collections.singletonList(ownership));
    when(_userManager.validateUserGroupMembership(any(), any())).thenReturn(true);
    when(_userManager.validateGroup(any())).thenReturn(true);
    when(_rampRuleDao.addRampRule(any())).thenReturn(1);
    assertThatCode(() -> _rampRuleService.createRule(requestDTO, user, false)).doesNotThrowAnyException();
  }

  @Test
  public void testInvalidImageName() {
    final String json = JSONUtils.readJsonFileAsString("image_management/image_ramp_rule_invalid.json");
    final ImageRampRuleRequestDTO requestDTO = _converterUtils.convertToDTO(json, ImageRampRuleRequestDTO.class);
    when(_imageTypeDao.getImageTypeByName(requestDTO.getImageName())).thenReturn(Optional.empty());
    assertThatCode(() -> _rampRuleService.createRule(requestDTO, "user", false))
        .hasMessageContaining("Invalid image type");
  }

  @Test
  public void testInvalidImageVersion() {
    final String json = JSONUtils.readJsonFileAsString("image_management/image_ramp_rule_invalid.json");
    final ImageRampRuleRequestDTO requestDTO = _converterUtils.convertToDTO(json, ImageRampRuleRequestDTO.class);
    when(_imageTypeDao.getImageTypeByName(requestDTO.getImageName())).thenReturn(Optional.of(new ImageType()));
    when(_imageVersionDao.isInvalidVersion(requestDTO.getImageName(), requestDTO.getImageVersion())).thenReturn(false);
    assertThatCode(() -> _rampRuleService.createRule(requestDTO, "user", false))
        .hasMessageContaining("Invalid image version");
  }

  @Test
  public void testNotAuthorizedUser() {
    String user = "user";
    String group = "group";
    final String json = JSONUtils.readJsonFileAsString("image_management/image_ramp_rule.json");
    final ImageRampRuleRequestDTO requestDTO = _converterUtils.convertToDTO(json, ImageRampRuleRequestDTO.class);
    when(_imageTypeDao.getImageTypeByName(requestDTO.getImageName())).thenReturn(Optional.of(new ImageType()));
    when(_imageVersionDao.isInvalidVersion(requestDTO.getImageName(), requestDTO.getImageVersion())).thenReturn(true);
    ImageOwnership ownership = new ImageOwnership();
    ownership.setOwner(group);
    when(_imageTypeDao.getImageTypeOwnership(requestDTO.getImageName())).thenReturn(Collections.singletonList(ownership));
    when(_userManager.validateUserGroupMembership(any(), any())).thenReturn(false);
    assertThatCode(() -> _rampRuleService.createRule(requestDTO, user, false))
        .hasMessageContaining("unauthorized user");
  }


}
