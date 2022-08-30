package azkaban.imagemgmt.services;

import azkaban.imagemgmt.daos.ImageTypeDao;
import azkaban.imagemgmt.daos.ImageVersionDao;
import azkaban.imagemgmt.daos.RampRuleDao;
import azkaban.imagemgmt.dto.ImageRampRuleRequestDTO;
import azkaban.imagemgmt.exception.ErrorCode;
import azkaban.imagemgmt.exception.ImageMgmtInvalidInputException;
import azkaban.imagemgmt.exception.ImageMgmtInvalidPermissionException;
import azkaban.imagemgmt.models.ImageOwnership;
import azkaban.imagemgmt.models.ImageRampRule;
import azkaban.imagemgmt.models.ImageType;
import azkaban.user.UserManager;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.log4j.Logger;

@Singleton
public class ImageRampRuleServiceImpl implements ImageRampRuleService {

  public static final Logger log = Logger.getLogger(ImageRampRuleServiceImpl.class);

  private final RampRuleDao rampRuleDao;
  private final ImageTypeDao imageTypeDao;
  private final ImageVersionDao imageVersionDao;
  private final UserManager userManager;

  @Inject
  public ImageRampRuleServiceImpl(RampRuleDao rampRuleDao,
                                  ImageTypeDao imageTypeDao,
                                  ImageVersionDao imageVersionDao,
                                  UserManager userManager) {
    this.rampRuleDao = rampRuleDao;
    this.imageTypeDao = imageTypeDao;
    this.imageVersionDao = imageVersionDao;
    this.userManager = userManager;
  }

  @Override
  public void createRule(ImageRampRuleRequestDTO rampRuleRequest, String ldapUser, boolean isAzkabanAdmin){
    // validate image_name and image_version
    final ImageType imageType = imageTypeDao
      .getImageTypeByName(rampRuleRequest.getImageName())
        .orElseThrow(() -> new ImageMgmtInvalidInputException(ErrorCode.NOT_FOUND, String.format("Unable to"
            + " fetch image type metadata. Invalid image type: %s.", rampRuleRequest.getImageName())));
    if (!this.imageVersionDao.isInvalidVersion(rampRuleRequest.getImageName(), rampRuleRequest.getImageVersion())) {
      throw new ImageMgmtInvalidInputException(ErrorCode.NOT_FOUND, String.format(
          "Unable to fetch image version metadata. Invalid image version: %s.", rampRuleRequest.getImageVersion()));
    }
    boolean isAuthorized = isAzkabanAdmin;
    //fetch owners from image_ownerships
    Set<String> imageOwnerships = imageTypeDao.getImageTypeOwnership(rampRuleRequest.getImageName()).stream()
        .map(ImageOwnership::getOwner).collect(Collectors.toSet());
    // validate if user has permission to create rule, if not azkaban admin user
    if (!imageOwnerships.isEmpty() &&
        (imageOwnerships.contains(ldapUser) || userManager.validateUserGroupMembership(ldapUser, imageOwnerships))) {
      isAuthorized = true;
    }
    if (!isAuthorized) {
      throw new ImageMgmtInvalidPermissionException(ErrorCode.UNAUTHORIZED,
          "Only image type owners would be authorized to create ramp rules, "
              + "unauthorized user " + ldapUser + " is not in " + imageOwnerships);
    }
    // convert ImageRampRule and insert new ramp rule into DB
    ImageRampRule rampRule = new ImageRampRule(rampRuleRequest.getRuleId(), rampRuleRequest.getImageName(),
        rampRuleRequest.getImageVersion(), imageOwnerships, false);
    rampRule.setCreatedBy(ldapUser);
    rampRuleDao.addRampRule(rampRule);
  }

  @Override
  public void createHpFlowRule(ImageRampRule rule) {

  }

  @Override
  public void deleteRule(String ruleId) {

  }

  @Override
  public void addFlowsToRule(List<String> flowIds, String ruleId) {

  }

  @Override
  public void updateVersionOnRule(String newVersion, String ruleId) {

  }
}
