package azkaban.imagemgmt.daos;

import azkaban.imagemgmt.models.ImageRampRule;


/**
* Data access object (DAO) for accessing image ramp rule that deny an image ramping version for flows.
* This interface defines add/remove/modify an image ramp rule.
* */
public interface RampRuleDao {

  boolean isHPRule(String ruleId);

  /*
  * Insert a new row into DB for ramp rule
  * */
  int addRampRule(ImageRampRule rule);

  String getOwners(String ruleId);

  void deleteRampRule(String ruleId);

}
