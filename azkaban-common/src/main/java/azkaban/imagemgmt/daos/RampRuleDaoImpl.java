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
import azkaban.imagemgmt.exception.ErrorCode;
import azkaban.imagemgmt.exception.ImageMgmtDaoException;
import azkaban.imagemgmt.models.ImageRampRule;
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

  private static String INSERT_RAMP_RULE = "INSERT into ramp_rules "
      + "(rule_name, image_name, image_version, owners, is_HP, created_by, created_on, modified_by, modified_on)"
      + " values (?, ?, ?, ?, ?, ?, ?, ?, ?)";

  @Inject
  public RampRuleDaoImpl(DatabaseOperator databaseOperator) {
    this.databaseOperator = databaseOperator;
  }

  @Override
  public boolean isHPFlowRule(final String ruleName) {
    return false;
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
      List<ImageRampRule> rampRules = databaseOperator.query(
          FetchRampRuleHandler.FETCH_RAMP_RULE_BY_ID, new FetchRampRuleHandler(), rampRule.getRuleName());
      if (!rampRules.isEmpty()) {
        LOG.error("Error in create ramp rule on duplicate ruleName: " + rampRule.getRuleName());
        throw new ImageMgmtDaoException(ErrorCode.BAD_REQUEST,
            "Error in create ramp rule on duplicate ruleName: " + rampRule.getRuleName());
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

  @Override
  public String getOwners(final String ruleName) {
    return null;
  }

  @Override
  public void deleteRampRule(final String ruleName) {

  }

  public static class FetchRampRuleHandler implements ResultSetHandler<List<ImageRampRule>> {

    private static final String FETCH_RAMP_RULE_BY_ID =
        "SELECT rule_name, image_name, image_version, owners, is_HP"
            + " FROM ramp_rules WHERE rule_name = ?";

    @Override
    public List<ImageRampRule> handle(final ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Collections.emptyList();
      }
      final List<ImageRampRule> imageRampRules = new ArrayList<>();
      do {
        final String ruleName = rs.getString("rule_name");
        final String imageName = rs.getString("image_name");
        final String imageVersion = rs.getString("image_version");
        final String owners = rs.getString("owners");
        final boolean isHP = rs.getBoolean("is_HP");
        Set<String> ownerLists = new HashSet<>(Arrays.asList(Strings.split(owners, ',')));
        final ImageRampRule imageRampRule = new ImageRampRule.Builder()
            .setRuleName(ruleName)
            .setImageName(imageName)
            .setImageVersion(imageVersion)
            .setOwners(ownerLists)
            .setHPRule(isHP)
            .build();
        imageRampRules.add(imageRampRule);
      } while (rs.next());
      return imageRampRules;
    }
  }
}
