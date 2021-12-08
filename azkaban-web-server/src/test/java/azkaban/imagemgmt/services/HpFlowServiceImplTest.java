package azkaban.imagemgmt.services;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import azkaban.imagemgmt.daos.HPFlowDao;
import azkaban.imagemgmt.daos.HPFlowDaoImpl;
import azkaban.imagemgmt.dto.HPFlowDTO;
import azkaban.imagemgmt.utils.ConverterUtils;
import azkaban.utils.JSONUtils;
import java.util.List;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class HpFlowServiceImplTest {
  private HPFlowDao hpFlowDao;
  private HPFlowService hpFlowService;
  private ObjectMapper objectMapper;
  private ConverterUtils converterUtils;

  @Before
  public void setup() {
    this.hpFlowDao = mock(HPFlowDaoImpl.class);
    this.hpFlowService = new HPFlowServiceImpl(hpFlowDao);
    this.objectMapper = new ObjectMapper();
    this.converterUtils = new ConverterUtils(objectMapper);
  }

  /**
   * Test to add high priority flows
   */
  @Test
  public void testAddHPFlows() {
    final String jsonPayLoad = JSONUtils.readJsonFileAsString("hp_flows/hp_flows.json");
    final HPFlowDTO hpFlowDTO = this.converterUtils.convertToDTO(jsonPayLoad, HPFlowDTO.class);
    final List<String> hpFlowList = ((HPFlowServiceImpl)(this.hpFlowService)).getFlowIdList(hpFlowDTO);
    hpFlowDTO.setCreatedBy("azkaban");
    when(this.hpFlowDao.addHPFlows(hpFlowList, "azkaban")).thenReturn(3);
    this.hpFlowService.addHPFlows(hpFlowDTO);
    // Verify that the list has flow names as intended.
    Assert.assertEquals("project1.flow1", hpFlowList.get(0));
    Assert.assertEquals("project_2.flow_2", hpFlowList.get(1));
    Assert.assertEquals("project-3.flow-3", hpFlowList.get(2));
  }

  /**
   * Negative test with empty list of flows.
   */
  @Test
  public void testAddHPFlowsEmpty() {
    final String jsonPayLoad = JSONUtils.readJsonFileAsString("hp_flows/hp_flow_invalid.json");
    final HPFlowDTO hpFlowDTO = this.converterUtils.convertToDTO(jsonPayLoad, HPFlowDTO.class);
    final List<String> hpFlowList = ((HPFlowServiceImpl)(this.hpFlowService)).getFlowIdList(hpFlowDTO);
    Assert.assertTrue(hpFlowList.isEmpty());
  }
}
