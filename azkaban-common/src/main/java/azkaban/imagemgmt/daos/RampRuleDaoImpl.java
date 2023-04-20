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

import azkaban.db.DatabaseOperator;
import azkaban.db.SQLTransaction;
import azkaban.imagemgmt.dto.RampRuleFlowsDTO.ProjectFlow;
import azkaban.imagemgmt.exception.ErrorCode;
import azkaban.imagemgmt.exception.ImageMgmtDaoException;
import azkaban.imagemgmt.models.ImageRampRule;
import azkaban.imagemgmt.models.RampRuleDenyList;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.log4j.Logger;
import org.bouncycastle.util.Strings;


/**
 * Dao Implementation for access DB of table ramp_rules.
 * Create/update/delete/get RampRule or partial RampRule metadata. {@link ImageRampRule}
 * */
@Singleton
public class RampRuleDaoImpl implements RampRuleDao {
  private static final Logger LOG = Logger.getLogger(RampRuleDaoImpl.class);
  private final DatabaseOperator databaseOperator;

  private static final String IMAGE_VERSION_DELIMITER = ":";

  private static final String INSERT_RAMP_RULE = "INSERT into ramp_rules "
      + "(rule_name, image_name, image_version, owners, is_HP, created_by, created_on, modified_by, modified_on)"
      + " values (?, ?, ?, ?, ?, ?, ?, ?, ?)";

  private static final String UPDATE_RULE_OWNERSHIP = "UPDATE ramp_rules "
      + "SET owners=?, modified_by=?, modified_on=? WHERE rule_name=?";

  private static final String UPDATE_VERSION_ON_RAMP_RULE = "UPDATE ramp_rules "
      + "SET image_version=?, modified_by=?, modified_on=? WHERE rule_name=?";

  private static final String DELETE_RULE_ON_RAMP_RULE = "DELETE from ramp_rules where rule_name = ?";

  private static final String DELETE_FLOW_DENY_LIST = "DELETE from flow_deny_lists where rule_name = ?";

  private static final String UPDATE_VERSION_ON_DENY_LIST = "UPDATE flow_deny_lists "
      + "SET deny_version=? WHERE rule_name=?";

  private static final String INSERT_FLOW_DENY_LIST = "INSERT into flow_deny_lists "
      + "(flow_id, deny_mode, deny_version, rule_name) values (?, ?, ?, ?)";

  private static final String INSERT_HP_DENY_LIST = "INSERT into flow_deny_lists "
      + "(flow_id, deny_mode, rule_name) values (?, ?, ?)";

  @Inject
  public RampRuleDaoImpl(final DatabaseOperator databaseOperator) {
    this.databaseOperator = databaseOperator;
  }

  @Override
  public boolean isExcludedByRampRule(final String flowName, final String imageName, final String imageVersion) {
    final String targetImageVersion = String.join(IMAGE_VERSION_DELIMITER, imageName, imageVersion);
    try {
      List<RampRuleDenyList> rampRuleDenyLists = databaseOperator.query(
          FetchFlowDenyListHandler.FETCH_FLOW_DENY_LIST_BY_FLOW_ID,
          new FetchFlowDenyListHandler(), flowName);
      for (RampRuleDenyList rampRuleDenyList : rampRuleDenyLists) {
        // denyMode.ALL means this flow is an HP flow,
        // if denyMode is not set to ALL then it must have denyVersion, then match given image version
        // the flow will be excluded too
        if (rampRuleDenyList.getDenyMode().equals(DenyMode.ALL)) {
          return true;
        } else if (rampRuleDenyList.getDenyVersion().equals(targetImageVersion)) {
          return true;
        }
      }
      return false;
    } catch (SQLException e) {
      LOG.error("fail to query ramp rule deny list: " + e);
      throw new ImageMgmtDaoException("fail to query ramp rule deny list: " + e.getMessage());
    }
  }

  /**
   * Insert new ramp rule into DB, check if duplicate ruleName exists first
   *
   * @param rampRule
   * @throws ImageMgmtDaoException
   * */
  @Override
  public int addRampRule(final ImageRampRule rampRule) {
    try {
      // duplicated rule should be forbidden as client side error, which needs a separate check to
      // avoid it falls into normal SQL Exception as server error.
      final ImageRampRule imageRampRule = databaseOperator.query(
          FetchRampRuleHandler.FETCH_RAMP_RULE_BY_ID, new FetchRampRuleHandler(), rampRule.getRuleName());
      if (imageRampRule != null) {
        LOG.error("Error in create ramp rule on duplicate ruleName: " + imageRampRule.getRuleName());
        throw new ImageMgmtDaoException(ErrorCode.BAD_REQUEST,
            "Error in create ramp rule on duplicate ruleName: " + imageRampRule.getRuleName());
      }
      return this.databaseOperator.update(INSERT_RAMP_RULE,
          rampRule.getRuleName(),
          rampRule.getImageName(),
          rampRule.getImageVersion(),
          rampRule.getOwners(),
          rampRule.isHPRule(),
          rampRule.getCreatedBy(),
          Timestamp.valueOf(LocalDateTime.now()),
          rampRule.getCreatedBy(),
          Timestamp.valueOf(LocalDateTime.now()));
    } catch (SQLException e) {
      LOG.error("Error in create ramp rule on DB" + rampRule);
      throw new ImageMgmtDaoException(ErrorCode.INTERNAL_SERVER_ERROR,
          "Error creating ramp rule " + rampRule.getRuleName() + "with " + e.getMessage());
    }
  }

  /**
   * update column owners from table ramp_rules along with modification metadata.
   *
   * @param newOwners
   * @param ruleName
   * @param modifiedBy
   * @return int - id of the DB entry
   * @throws ImageMgmtDaoException
   */
  @Override
  public int updateOwnerships(final String newOwners, final String ruleName, final String modifiedBy) {
    try {
      // validation should already done
      return this.databaseOperator.update(UPDATE_RULE_OWNERSHIP,
          newOwners, modifiedBy, Timestamp.valueOf(LocalDateTime.now()), ruleName);
    } catch (SQLException e) {
      LOG.error("Error in updating ownership " + newOwners);
      throw new ImageMgmtDaoException(ErrorCode.INTERNAL_SERVER_ERROR,
          "Error in updating ownership with " + e.getMessage());
    }
  }

  /**
   * Query table ramp_rules to get owners of Ramp rule.
   * Serves as the source of truth for rule ownership management.
   *
   * @param ruleName - ruleName in {@see ImageRampRule}
   * @return owners of the ramp rule.
   */
  @Override
  public Set<String> getOwners(final String ruleName) {
    final ImageRampRule rampRule = getRampRule(ruleName);
    String owners = rampRule.getOwners();
    return new HashSet<>(Arrays.asList(owners.split(",")));
  }

  /**
   * Get Unique ramp rule based on given ruleName.
   * Fetched results would be converted into model {@link ImageRampRule}
   *
   * @param ruleName - ruleName in {@link ImageRampRule}
   * @return the ramp rule.
   */
  @Override
  public ImageRampRule getRampRule(String ruleName) {
    try {
      final ImageRampRule rampRule = databaseOperator.query(FetchRampRuleHandler.FETCH_RAMP_RULE_BY_ID,
          new FetchRampRuleHandler(), ruleName);
      if (rampRule == null) {
        LOG.error("Can not find ramp rule at the ruleName: " + ruleName);
        throw new ImageMgmtDaoException(ErrorCode.BAD_REQUEST, "ramp rule not found with ruleName: " + ruleName);
      }
      return rampRule;
    } catch (SQLException e) {
      LOG.error("Unable to fetch the ramp rule", e);
      throw new ImageMgmtDaoException("failed to fetch Ramp Rule from DB." + e);
    }
  }

  @Override
  public void deleteRampRule(final String ruleName) {
    final SQLTransaction<Long> deleteRampRuleAndFlowDenyList = transOperator -> {
      transOperator.update(DELETE_RULE_ON_RAMP_RULE, ruleName);
      transOperator.update(DELETE_FLOW_DENY_LIST, ruleName);
      transOperator.getConnection().commit();
      return transOperator.getLastInsertId();
    };
    try {
      databaseOperator.transaction(deleteRampRuleAndFlowDenyList);
    } catch (SQLException e) {
      LOG.error("failed to delete ramp rule: " + ruleName);
      throw new ImageMgmtDaoException("Error in deleting data based on ramp rule: " + ruleName);
    }
  }

  /**
   * Map FlowId (project.flow) with denying information (denyMode or denyVersion) and insert them into DB.
   * Deduplication performed in same rule with same flow to avoid DB inflation.
   *
   * @param flowIds
   * @param ruleName
   * @return int - id of the DB entry
   */
  @Override
  public int addFlowDenyInfo(final List<ProjectFlow> flowIds, final String ruleName) {
    // query ramp_rules to get ramp rule information
    // based on ramp rule to generate mappings with flowId as key
    final SQLTransaction<Long> fetchRampRuleAndUpdateDenyList = transOperator -> {
      final ImageRampRule imageRampRule =
          transOperator.query(FetchRampRuleHandler.FETCH_RAMP_RULE_BY_ID, new FetchRampRuleHandler(), ruleName);
      if (imageRampRule == null) {
        LOG.error("fail to find the existing ruleName: " + ruleName);
        throw new ImageMgmtDaoException("ramp rule not found with ruleName: " + ruleName);
      }
      // insert HP flows: <flowId, denyMode.ALL, ruleName> based on whether HP flow
      if (imageRampRule.isHPRule()) {
        LOG.info("handling add flows for HP Flow Rule: " + ruleName);
        for (final ProjectFlow flowId : flowIds) {
          // avoid duplicate insert HP flows, use <flowId, denyMode, ruleName> to identity duplicates.
          // in case one rule deleted, the others still exist
          if (transOperator.query(
                FetchFlowDenyListHandler.FETCH_FLOW_DENY_LIST_BY_FLOW_ID_DENY_MODE_RULE_NAME,
                new FetchFlowDenyListHandler(), flowId.toString(), DenyMode.ALL.name(), ruleName)
              .isEmpty()) {
            transOperator.update(INSERT_HP_DENY_LIST, flowId.toString(), DenyMode.ALL.name(), ruleName);
          }
        }
      } else {
        // insert normal flows: <flowId, denyMode.PARTIAL, ruleName>
        final String denyVersion = String.join(IMAGE_VERSION_DELIMITER, imageRampRule.getImageName(), imageRampRule.getImageVersion());
        LOG.info("handling add flows for normal Flow Rule: " + ruleName + " with denyVersion: " + denyVersion);
        for (final ProjectFlow flowId : flowIds) {
          // avoid duplicate insert, use <flowId, denyVersion, ruleName> to identity duplicates.
          // in case one got deleted, the others should not get impacted
          if (transOperator.query(
                  FetchFlowDenyListHandler.FETCH_FLOW_DENY_LIST_BY_FLOW_ID_DENY_VERSION_RULE_NAME,
                  new FetchFlowDenyListHandler(), flowId.toString(), denyVersion, ruleName)
              .isEmpty()) {
            transOperator.update(INSERT_FLOW_DENY_LIST, flowId.toString(), DenyMode.PARTIAL.name(), denyVersion, ruleName);
          }
        }
      }
      // end if
      transOperator.getConnection().commit();
      return transOperator.getLastInsertId();
    };
    // end SQL transaction operator
    try {
      int batchInsertId = this.databaseOperator.transaction(fetchRampRuleAndUpdateDenyList).intValue();
      if (batchInsertId == 0) {
        LOG.warn(String.format("creating no new flow deny list based on rule: %s, "
                + "flowList: %s. Might due to deny rule already exists", ruleName, flowIds));
        throw new ImageMgmtDaoException(ErrorCode.BAD_REQUEST,
            String.format("flows already exists in the rule: %s, " + "flowList: %s.", ruleName,
                flowIds));
      }
      if (batchInsertId < 0) {
        LOG.warn(String.format("Error on inserting into DB based on rule: %s, "
            + "flowList: %s.", ruleName, flowIds));
        throw new ImageMgmtDaoException(ErrorCode.BAD_REQUEST,
            String.format("Exception while creating flow deny list based on rule: %s, " + "flowList: %s.", ruleName,
                flowIds));
      }
      return batchInsertId;
    } catch (final SQLException e) {
      LOG.error("Unable to create the flow deny list metadata", e);
      throw new ImageMgmtDaoException("Exception while creating the flow deny list metadata: " + e.getMessage());
    }
  }

  /**
   * Update version on the rule, both table flow_deny_lists and ramp_rules got refreshed data.
   * It will fetch ramp_rules to get metadata and operate UPDATE on both in a single transaction.
   *
   * @param newVersion
   * @param ruleName - ruleName in {@see ImageRampRule}
   * @param user
   * @throws azkaban.imagemgmt.exception.ImageMgmtDaoException
   */
  @Override
  public void updateVersionOnRule(final String newVersion, final String ruleName, final String user) {
    final SQLTransaction<Long> fetchRampRuleAndUpdateDenyList = transOperator -> {
      final ImageRampRule imageRampRule =
          transOperator.query(FetchRampRuleHandler.FETCH_RAMP_RULE_BY_ID, new FetchRampRuleHandler(), ruleName);
      final String newDenyVersion = String.join(IMAGE_VERSION_DELIMITER, imageRampRule.getImageName(), newVersion);
      if (!newVersion.equals(imageRampRule.getImageVersion())) {
        transOperator.update(UPDATE_VERSION_ON_RAMP_RULE,
            newVersion, user, Timestamp.valueOf(LocalDateTime.now()), ruleName);
        transOperator.update(UPDATE_VERSION_ON_DENY_LIST, newDenyVersion, ruleName);
      }
      transOperator.getConnection().commit();
      return transOperator.getLastInsertId();
    };
    // end SQL transaction operator
    try {
      this.databaseOperator.transaction(fetchRampRuleAndUpdateDenyList);
    } catch (final SQLException e) {
      LOG.error("Unable to update the ramp rule metadata", e);
      throw new ImageMgmtDaoException("Unable to update the ramp rule version: " + e.getMessage());
    }
  }

  public static class FetchRampRuleHandler implements ResultSetHandler<ImageRampRule> {

    private static final String FETCH_RAMP_RULE_BY_ID =
        "SELECT rule_name, image_name, image_version, owners, is_HP"
            + " FROM ramp_rules WHERE rule_name = ?";

    @Override
    public ImageRampRule handle(final ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return null;
      }
      final String ruleName = rs.getString("rule_name");
      final String imageName = rs.getString("image_name");
      final String imageVersion = rs.getString("image_version");
      final String owners = rs.getString("owners");
      final boolean isHP = rs.getBoolean("is_HP");
      Set<String> ownerLists = new HashSet<>(Arrays.asList(Strings.split(owners, ',')));
      return new ImageRampRule.Builder()
          .setRuleName(ruleName)
          .setImageName(imageName)
          .setImageVersion(imageVersion)
          .setOwners(ownerLists)
          .setHPRule(isHP)
          .build();
    }
  }

  public static class FetchFlowDenyListHandler implements ResultSetHandler<List<RampRuleDenyList>> {

    private static final String FETCH_FLOW_DENY_LIST_BY_FLOW_ID_DENY_MODE_RULE_NAME =
        "SELECT flow_id, deny_mode, deny_version, rule_name"
            + " FROM flow_deny_lists WHERE flow_id = ? and deny_mode = ? and rule_name = ?";
    private static final String FETCH_FLOW_DENY_LIST_BY_FLOW_ID_DENY_VERSION_RULE_NAME =
        "SELECT flow_id, deny_mode, deny_version, rule_name"
            + " FROM flow_deny_lists WHERE flow_id = ? and deny_version = ? and rule_name = ?";
    private static final String FETCH_FLOW_DENY_LIST_BY_FLOW_ID =
        "SELECT flow_id, deny_mode, deny_version, rule_name"
            + " FROM flow_deny_lists WHERE flow_id = ?";

    @Override
    public List<RampRuleDenyList> handle(final ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Collections.emptyList();
      }
      final List<RampRuleDenyList> denyLists = new ArrayList<>();
      do {
        final String ruleName = rs.getString("rule_name");
        final String flowId = rs.getString("flow_id");
        final String denyMode = rs.getString("deny_mode");
        final String denyVersion = rs.getString("deny_version");
        final RampRuleDenyList denyList = new RampRuleDenyList.Builder()
            .setFlowId(flowId)
            .setRuleName(ruleName)
            .setDenyMode(denyMode)
            .setDenyVersion(denyVersion)
            .build();
        denyLists.add(denyList);
      } while (rs.next());
      return denyLists;
    }
  }
}
