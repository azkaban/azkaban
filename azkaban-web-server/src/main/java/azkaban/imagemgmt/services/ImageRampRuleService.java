package azkaban.imagemgmt.services;

import azkaban.imagemgmt.dto.ImageRampRuleRequestDTO;
import azkaban.imagemgmt.models.ImageRampRule;
import java.util.List;

/**
 * Interface for ImageRampRule Service,
 * support the basic operation to add/modify/delete ramp rule
 * */
public interface ImageRampRuleService {

  /**
   * Create a normal exclusive Rule for a certain version of an image type,
   * validation performed before insert into DB.
   * throws @ImageMgmtException
   * */
  void createRule(ImageRampRuleRequestDTO rule, String ldapUser, boolean isAzkabanAdmin);

  void createHpFlowRule(ImageRampRule rule);

  void deleteRule(String ruleId);

  void addFlowsToRule(List<String> flowIds, String ruleId);

  void updateVersionOnRule(String newVersion, String ruleId);
}
