package azkaban.imagemgmt.servlets;

import azkaban.imagemgmt.dto.ImageRampRuleRequestDTO;
import azkaban.imagemgmt.exception.ErrorCode;
import azkaban.imagemgmt.exception.ImageMgmtException;
import azkaban.imagemgmt.services.ImageRampRuleService;
import azkaban.imagemgmt.utils.ConverterUtils;
import azkaban.server.HttpRequestUtils;
import azkaban.server.session.Session;
import azkaban.webapp.AzkabanWebServer;
import azkaban.webapp.servlet.LoginAbstractAzkabanServlet;
import java.io.IOException;
import java.util.ArrayList;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ImageRampRuleServlet extends LoginAbstractAzkabanServlet {
  private static final Logger LOG = LoggerFactory.getLogger(ImageRampRuleServlet.class);

  private ImageRampRuleService imageRampRuleService;
  private ConverterUtils utils;

  private final static String CREATE_RULE_URI = "/imageRampRule/createRule";

  public ImageRampRuleServlet() {
    super(new ArrayList<>());
  }

  @Override
  public void init(final ServletConfig config) throws ServletException {
    super.init(config);
    final AzkabanWebServer server = (AzkabanWebServer) getApplication();
    this.imageRampRuleService = server.getImageRampRuleService();
    this.utils = server.getConverterUtils();
  }

  @Override
  protected void handleGet(HttpServletRequest req, HttpServletResponse resp, Session session)
      throws ServletException, IOException {

  }

  @Override
  protected void handlePost(HttpServletRequest req, HttpServletResponse resp, Session session)
      throws ServletException, IOException {
      String requestURI = req.getRequestURI();
      String userId = session.getUser().getUserId();
      boolean isAzkabanAdmin = isAzkabanAdmin(session.getUser());
      if (CREATE_RULE_URI.equals(requestURI)) {
        LOG.info("handle request from post uri: " + requestURI);
        handleCreateRampRule(req, resp, userId, isAzkabanAdmin);
      }
  }

  // create an exclusive rule for a certain version of the image,
  // any failure would be wrapped into ImageMgmtException with different ErrorCode,
  // as long with the detailed error message back to the client.
  // Successful call would return CREATED(201)
  private void handleCreateRampRule(HttpServletRequest req,
                                    HttpServletResponse resp,
                                    String userId,
                                    boolean isAzkabanAdmin)
                                    throws ServletException {
    String requestBody = HttpRequestUtils.getBody(req);
    ImageRampRuleRequestDTO rampRuleRequestDTO;
    try {
      // while converting to requestDTO, validation on json/required parameters would be performed.
      rampRuleRequestDTO = utils.convertToDTO(requestBody, ImageRampRuleRequestDTO.class);
      rampRuleRequestDTO.setCreatedBy(userId);
      imageRampRuleService.createRule(rampRuleRequestDTO, userId, isAzkabanAdmin);
      resp.setStatus(HttpStatus.SC_CREATED);
    } catch (ImageMgmtException e) {
      LOG.error("failed to create a rampRule: " + requestBody);
      resp.setStatus(e.getErrorCode().getCode(), e.getMessage());
    }
  }


}
