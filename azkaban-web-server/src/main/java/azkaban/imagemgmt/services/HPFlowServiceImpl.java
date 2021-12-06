package azkaban.imagemgmt.services;

import azkaban.imagemgmt.daos.HPFlowDao;
import azkaban.imagemgmt.dto.HPFlowDTO;
import azkaban.imagemgmt.exception.ImageMgmtException;
import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.log4j.Logger;

/**
 * This service layer implementation exposes methods for delegation and
 * processing logic for high priority flow APIs. The requests are routed
 * to respective DAO layer for data access.
 */
@Singleton
public class HPFlowServiceImpl implements HPFlowService {
  public static final Logger log = Logger.getLogger(HPFlowServiceImpl.class);

  private final HPFlowDao hpFlowDao;

  @Inject
  public HPFlowServiceImpl(final HPFlowDao hpFlowDao) {
    this.hpFlowDao = hpFlowDao;
  }

  /**
   * Add one or more HP flows.
   * @param hpFlowDTO
   * @throws ImageMgmtException
   */
  @Override
  public List<String> addHPFlows(final HPFlowDTO hpFlowDTO) throws ImageMgmtException {
    // Get list of flows from the DTO
    final List<String> flowIdList = hpFlowDTO.getFlowIdList();
    this.hpFlowDao.addHPFlows(flowIdList, hpFlowDTO.getCreatedBy());
    return flowIdList;
  }

  @VisibleForTesting
  List<String> getFlowIdList(final HPFlowDTO hpFlowDTO) {
    return hpFlowDTO.getFlowIdList();
  }
}
