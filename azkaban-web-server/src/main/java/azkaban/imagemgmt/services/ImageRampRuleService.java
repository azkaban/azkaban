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

import azkaban.imagemgmt.dto.RampRuleOwnershipDTO;
import azkaban.imagemgmt.dto.ImageRampRuleRequestDTO;
import azkaban.imagemgmt.exception.ImageMgmtException;
import azkaban.imagemgmt.models.ImageRampRule;
import azkaban.user.User;
import java.util.List;

/**
 * Interface for ImageRampRule Service,
 * support the basic operation to add/modify/delete ramp rule
 * */
public interface ImageRampRuleService {

  /**
   * Create a normal exclusive Rule for a certain version of an image type,
   * validation performed based on {@link ImageRampRuleRequestDTO} and convert to {@link ImageRampRule}
   * before insert into DB.
   *
   * @param rampRuleRequestDTO
   * @param ldapUser
   * @throws ImageMgmtException
   * */
  void createRule(final ImageRampRuleRequestDTO rampRuleRequestDTO, final User ldapUser);

  /**
   * Create an HP Flow exclusive Rule to denying all image Types,
   * validation performed based on {@link RampRuleOwnershipDTO} and convert to {@link ImageRampRule}
   *
   * @param hpFlowRuleOwnershipRequestDTO
   * @param user
   * @throws ImageMgmtException
   * */
  void createHpFlowRule(final RampRuleOwnershipDTO hpFlowRuleOwnershipRequestDTO, final User user);

  /**
   * Update the rule with ownerships, only existing owners have the permission to add/remove new owners,
   * ownerships should be provided using "," as delimiter, otherwise validation will fail.
   * validation performed based on {@link RampRuleOwnershipDTO} and invoke DB.
   *
   * @param RuleOwnershipDTO
   * @param user
   * @param operationType
   * @throws ImageMgmtException
   * @return*/
  String updateRuleOwnership(final RampRuleOwnershipDTO RuleOwnershipDTO, final User user,
      final OperationType operationType);

  void deleteRule(final String ruleName);

  void addFlowsToRule(final List<String> flowIds, final String ruleName);

  void updateVersionOnRule(final String newVersion, final String ruleName);

  enum OperationType {
    ADD(),
    REMOVE()
  }
}
