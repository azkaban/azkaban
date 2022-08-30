package azkaban.imagemgmt.daos;

import azkaban.db.DatabaseOperator;
import azkaban.imagemgmt.exception.ErrorCode;
import azkaban.imagemgmt.exception.ImageMgmtDaoException;
import azkaban.imagemgmt.exception.ImageMgmtException;
import azkaban.imagemgmt.exception.ImageMgmtInvalidInputException;
import azkaban.imagemgmt.models.ImageRampRule;
import azkaban.imagemgmt.models.ImageType;
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


@Singleton
public class RampRuleDaoImpl implements RampRuleDao {
  private static final Logger LOG = Logger.getLogger(RampRuleDaoImpl.class);
  private final DatabaseOperator databaseOperator;

  private static String INSERT_RAMP_RULE = "INSERT into ramp_rules "
      + "(rule_id, image_name, image_version, owners, is_HP, created_by, created_on) values (?, ?, ?, ?, ?, ?, ?)";

  @Inject
  public RampRuleDaoImpl(DatabaseOperator databaseOperator) {
    this.databaseOperator = databaseOperator;
  }

  @Override
  public boolean isHPRule(String ruleId) {
    return false;
  }

  @Override
  public int addRampRule(ImageRampRule rampRule) {
    try {
      List<ImageRampRule> rampRules = databaseOperator.query(
          FetchRampRuleHandler.FETCH_RAMP_RULE_BY_ID, new FetchRampRuleHandler(), rampRule.getRuleId());
      if (!rampRules.isEmpty()) {
        LOG.error("Error in create ramp rule on duplicate ruleId: " + rampRule.getRuleId());
        throw new ImageMgmtException(ErrorCode.BAD_REQUEST,
            "Error in create ramp rule on duplicate ruleId: " + rampRule.getRuleId());
      }
      return this.databaseOperator.update(INSERT_RAMP_RULE,
          rampRule.getRuleId(),
          rampRule.getImageName(),
          rampRule.getImageVersion(),
          rampRule.getOwners(),
          rampRule.isHPRule(),
          rampRule.getCreatedBy(),
          Timestamp.valueOf(LocalDateTime.now()));
    } catch (SQLException e) {
      LOG.error("Error in create ramp rule on DB" + rampRule);
      throw new ImageMgmtDaoException(ErrorCode.INTERNAL_SERVER_ERROR,
          "Error creating ramp rule " + rampRule.getRuleId() + "with " + e.getMessage());
    }
  }

  @Override
  public String getOwners(String ruleId) {
    return null;
  }

  @Override
  public void deleteRampRule(String ruleId) {

  }

  public static class FetchRampRuleHandler implements ResultSetHandler<List<ImageRampRule>> {

    private static final String FETCH_RAMP_RULE_BY_ID =
        "SELECT rule_id, image_name, image_version, owners, is_HP"
            + " FROM ramp_rules WHERE rule_id = ?";

    @Override
    public List<ImageRampRule> handle(final ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Collections.emptyList();
      }
      final List<ImageRampRule> imageRampRules = new ArrayList<>();
      do {
        final String ruleId = rs.getString("rule_id");
        final String imageName = rs.getString("image_name");
        final String imageVersion = rs.getString("image_version");
        final String owners = rs.getString("owners");
        final boolean isHP = rs.getBoolean("is_HP");
        Set<String> ownerLists = new HashSet<>(Arrays.asList(Strings.split(owners, ',')));
        final ImageRampRule imageRampRule = new ImageRampRule(ruleId, imageName, imageVersion, ownerLists, isHP);
        imageRampRules.add(imageRampRule);
      } while (rs.next());
      return imageRampRules;
    }
  }
}
