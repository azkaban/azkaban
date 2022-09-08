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
package azkaban.imagemgmt.daos;

import azkaban.imagemgmt.models.ImageRampRule;
import java.util.Set;


/**
* Data access object (DAO) for accessing image ramp rule that deny an image ramping version for flows.
* This interface defines add/remove/modify/get an image ramp rule.
* */
public interface RampRuleDao {

  /**
   * Query table ramp_rules to check whether a HPFlowRule .
   *
   * @param ruleName - ruleName in {@see ImageRampRule}
   * @return true - if ramp rule is setup for HP Flows, denying any ramp up plan;
   *         false - if ramp rule is normal rule, regulate only on certain image version.
   */
  boolean isHPFlowRule(final String ruleName);

  /**
   * Insert Image Ramp Rule metadata into DB.
   *
   * @param rule - RampRule model created in {@see ImageRampRuleServiceImpl}
   * @return int - id of the DB entry
   * @throws azkaban.imagemgmt.exception.ImageMgmtDaoException
   */
  int addRampRule(final ImageRampRule rule);

  /**
   * Update owners metadata into table ramp_rules.
   *
   * @param newOwners
   * @param ruleName
   * @param modifiedBy
   * @return int - id of the DB entry
   * @throws azkaban.imagemgmt.exception.ImageMgmtDaoException
   */
  int updateOwnerships(final String newOwners, final String ruleName, final String modifiedBy);

  /**
   * Query table ramp_rules to get owners of Ramp rule.
   * Serves as the source of truth for rule ownership management.
   *
   * @param ruleName - ruleName in {@see ImageRampRule}
   * @return owners of the ramp rule.
   */
  Set<String> getOwners(final String ruleName);

  /**
   * delete the ramp_rules to get owners of Ramp rule.
   *
   * @param ruleName - ruleName in {@see ImageRampRule}
   * @throws azkaban.imagemgmt.exception.ImageMgmtDaoException
   */
  void deleteRampRule(final String ruleName);

}
