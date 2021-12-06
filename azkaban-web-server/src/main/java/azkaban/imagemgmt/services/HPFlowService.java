package azkaban.imagemgmt.services;

import azkaban.imagemgmt.dto.HPFlowDTO;
import azkaban.imagemgmt.exception.ImageMgmtException;
import java.util.List;


/**
 * This service layer interface exposes methods for delegation and processing
 * logic for high priority flow APIs. The requests are routed to respective
 * DAO layer for data access.
 */
public interface HPFlowService {

  /**
   * Add one or more HP flows.
   * @param hpFlowDTO
   * @throws ImageMgmtException
   */
  List<String> addHPFlows(final HPFlowDTO hpFlowDTO) throws ImageMgmtException;
}
