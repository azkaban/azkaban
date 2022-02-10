package azkaban.imagemgmt.daos;

import azkaban.executor.ExecutableFlow;
import java.util.List;


/**
 * Data access object (DAO) for accessing high priority flow metadata.
 * This interface defines add/remove/get methods for high priority flows.
 */
public interface HPFlowDao {
  /**
   * isHPFlowOwner
   * @param userId : owner of high priority flows.
   * @return : True if user is owner, false otherwise.
   */
  boolean isHPFlowOwner(final String userId);

  /**
   * addHPFlows
   * @param flowIds : list of flows.
   * @param userId : userId of owner who is adding the HP flows.
   */
  int addHPFlows(final List<String> flowIds, final String userId);

  /**
   * isHPFlow
   * @param flow : Given a flow, finds if it is a high priority flow.
   * @return
   */
  boolean isHPFlow(final ExecutableFlow flow);
}
