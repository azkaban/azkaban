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

import azkaban.executor.ExecutableFlow;
import azkaban.imagemgmt.models.ImageRampRule;
import azkaban.imagemgmt.dto.RampRuleFlowsDTO.ProjectFlow;
import java.util.List;
import java.util.Set;


/**
* Data access object (DAO) for accessing image ramp rule that deny an image ramping version for flows.
* This interface defines add/remove/modify/get an image ramp rule.
* */
public interface RampRuleDao {

  /**
   * Query table flow_deny_lists to check whether a given flow is defined under a Ramp rule under two cases:
   * - having {@link DenyMode#ALL}
   * - having {@link DenyMode#PARTIAL} and with a deny_version (imageName.imageVersion)
   *
   * @param flowName - flowName in {@link ExecutableFlow#getFlowName()}
   * @param imageName - image type name
   * @param imageVersion - image type version
   * @return true - if given flow is regulated under a ramp rule;
   *         false - otherwise.
   */
  boolean isExcludedByRampRule(final String flowName, final String imageName, final String imageVersion);

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
   * Query table ramp_rules to get image ramp rule associated with given ruleName.
   *
   * @param ruleName - ruleName in {@see ImageRampRule}
   * @return image ramp rule.
   */
  ImageRampRule getRampRule(final String ruleName);

  /**
   * delete the ramp_rules to get owners of Ramp rule.
   *
   * @param ruleName - ruleName in {@see ImageRampRule}
   * @throws azkaban.imagemgmt.exception.ImageMgmtDaoException
   */
  void deleteRampRule(final String ruleName);

  /**
   * create one to one mappings from flow to deny image information into table flow_deny_lists.
   * flowIds will be converted into format project.flow to match {@link ExecutableFlow#getFlowName()}.
   *
   * @param flowIds - list of flowIds in {@link ProjectFlow}
   * @param ruleName - ruleName in {@see ImageRampRule}
   * @throws azkaban.imagemgmt.exception.ImageMgmtDaoException
   */
  int addFlowDenyInfo(final List<ProjectFlow> flowIds, final String ruleName);

  /**
   * Update version on the rule, both table flow_deny_lists and ramp_rules got refreshed data.
   *
   * @param version - new version to be updated
   * @param ruleName - ruleName in {@see ImageRampRule}
   * @param user
   * @throws azkaban.imagemgmt.exception.ImageMgmtDaoException
   */
  void updateVersionOnRule(final String version, final String ruleName, String user);

  enum DenyMode {
    ALL, // deny all versions, used for HP flow ramp rule
    PARTIAL // deny partial versions, versions need to be specified in normal ramp rule
  }
}
